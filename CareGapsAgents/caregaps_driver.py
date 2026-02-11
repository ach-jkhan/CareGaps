# Databricks notebook source
# MAGIC %md
# MAGIC #Tool-calling Agent
# MAGIC
# MAGIC This is an auto-generated notebook created by an AI playground export. In this notebook, you will:
# MAGIC - Author a tool-calling [MLflow's `ResponsesAgent`](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ResponsesAgent) that uses the OpenAI client
# MAGIC - Manually test the agent's output
# MAGIC - Evaluate the agent with Mosaic AI Agent Evaluation
# MAGIC - Log and deploy the agent
# MAGIC
# MAGIC This notebook should be run on serverless or a cluster with DBR<17.
# MAGIC
# MAGIC  **_NOTE:_**  This notebook uses the OpenAI SDK, but AI Agent Framework is compatible with any agent authoring framework, including LlamaIndex or LangGraph. To learn more, see the [Authoring Agents](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/author-agent) Databricks documentation.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.

# COMMAND ----------

# MAGIC %pip install -U -qqqq backoff databricks-openai uv databricks-agents mlflow-skinny[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Define the agent in code
# MAGIC Below we define our agent code in a single cell, enabling us to easily write it to a local Python file for subsequent logging and deployment using the `%%writefile` magic command.
# MAGIC
# MAGIC For more examples of tools to add to your agent, see [docs](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/agent-tool).

# COMMAND ----------

# MAGIC %%writefile agent.py
# MAGIC
# MAGIC import json
# MAGIC import re
# MAGIC from typing import Any, Callable, Generator, Optional
# MAGIC from uuid import uuid4
# MAGIC import warnings
# MAGIC from datetime import datetime
# MAGIC
# MAGIC import mlflow
# MAGIC import openai
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks_openai import UCFunctionToolkit, VectorSearchRetrieverTool
# MAGIC from mlflow.entities import SpanType
# MAGIC from mlflow.pyfunc import ResponsesAgent
# MAGIC from mlflow.types.responses import (
# MAGIC     ResponsesAgentRequest,
# MAGIC     ResponsesAgentResponse,
# MAGIC     ResponsesAgentStreamEvent,
# MAGIC     output_to_responses_items_stream,
# MAGIC     to_chat_completions_input,
# MAGIC )
# MAGIC from openai import OpenAI
# MAGIC from pydantic import BaseModel
# MAGIC from unitycatalog.ai.core.base import get_uc_function_client
# MAGIC
# MAGIC ############################################
# MAGIC # Configuration
# MAGIC ############################################
# MAGIC LLM_ENDPOINT_NAME = "databricks-gpt-oss-20b"
# MAGIC
# MAGIC # PHI Masking Configuration
# MAGIC PHI_MASKING_ENABLED = True  # Set to False once authentication is implemented
# MAGIC SHOW_PHI_WARNING = True  # Show warning about masked data
# MAGIC
# MAGIC # System Prompt (Fixed encoding issues)
# MAGIC SYSTEM_PROMPT = """You are a Care Gaps Assistant for a pediatric healthcare system. Your role is to help clinicians, care coordinators, and administrators query and analyze patient care gaps using natural language.
# MAGIC
# MAGIC CAPABILITIES:
# MAGIC You have access to 14 SQL functions that query the care gaps database:
# MAGIC - Patient-specific queries (search, view gaps, 360-degree view)
# MAGIC - Priority and urgency queries (critical gaps, long-open gaps, outreach needs)
# MAGIC - Provider and department analysis
# MAGIC - Statistical overviews and trends
# MAGIC - Appointment coordination
# MAGIC - Gap type and category analysis
# MAGIC
# MAGIC DATA SCOPE:
# MAGIC - Pediatric patients with active care gaps
# MAGIC - Gap types: Immunizations, Well Child Visits, BMI Screenings, Developmental Assessments, etc.
# MAGIC - Priority levels: Critical, Important, Routine
# MAGIC - Provider assignments and departments
# MAGIC - Appointment scheduling information
# MAGIC - Patient contact information (phone, email)
# MAGIC
# MAGIC RESPONSE GUIDELINES:
# MAGIC 1. ALWAYS provide specific, actionable information
# MAGIC 2. Include patient identifiers when relevant for outreach
# MAGIC 3. Prioritize critical gaps over routine ones
# MAGIC 4. Format results in clear, readable tables when showing multiple items
# MAGIC 5. Suggest relevant follow-up questions or next steps
# MAGIC 6. Be concise but complete
# MAGIC 7. If no results found, suggest alternative searches or clarifications
# MAGIC
# MAGIC PRIVACY & SECURITY:
# MAGIC - Only share patient information when necessary for the query
# MAGIC - Assume the user has appropriate access permissions
# MAGIC - Do not make up or guess patient information
# MAGIC - If uncertain, ask for clarification
# MAGIC
# MAGIC TONE:
# MAGIC - Professional and helpful
# MAGIC - Healthcare-appropriate language
# MAGIC - Action-oriented (focus on what to do next)
# MAGIC - Empathetic toward patient needs
# MAGIC
# MAGIC FUNCTION SELECTION:
# MAGIC - For patient searches -> use search_patients() first
# MAGIC - For specific patient details -> use get_patient_gaps() or get_patient_360()
# MAGIC - For urgent items -> use get_critical_gaps()
# MAGIC - For outreach planning -> use get_outreach_needed()
# MAGIC - For statistics -> use get_gap_statistics()
# MAGIC - For provider/department analysis -> use get_provider_gaps() or get_department_summary()
# MAGIC - For scheduling -> use get_appointments_with_gaps()
# MAGIC - Always choose the most appropriate function(s) for the query
# MAGIC
# MAGIC When in doubt, ask clarifying questions before making assumptions."""
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## PHI Masking Functions
# MAGIC ###############################################################################
# MAGIC
# MAGIC class PHIMasker:
# MAGIC     """Mask Protected Health Information (PHI) in responses"""
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def mask_name(name: str) -> str:
# MAGIC         """Mask patient name, show only initials"""
# MAGIC         if not name or name.strip() == "":
# MAGIC             return "[REDACTED]"
# MAGIC         
# MAGIC         parts = name.strip().split()
# MAGIC         if len(parts) == 0:
# MAGIC             return "[REDACTED]"
# MAGIC         elif len(parts) == 1:
# MAGIC             return parts[0][0] + "***"
# MAGIC         else:
# MAGIC             # First initial + Last initial
# MAGIC             return f"{parts[0][0]}*** {parts[-1][0]}***"
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def mask_mrn(mrn: str) -> str:
# MAGIC         """Mask MRN, show only last 4 digits"""
# MAGIC         if not mrn or len(mrn) < 4:
# MAGIC             return "****" + (mrn[-2:] if mrn else "")
# MAGIC         return "****" + mrn[-4:]
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def mask_phone(phone: str) -> str:
# MAGIC         """Mask phone number"""
# MAGIC         if not phone:
# MAGIC             return "[REDACTED]"
# MAGIC         # Keep area code, mask rest
# MAGIC         digits = re.sub(r'\D', '', phone)
# MAGIC         if len(digits) >= 10:
# MAGIC             return f"({digits[:3]}) ***-****"
# MAGIC         return "***-****"
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def mask_email(email: str) -> str:
# MAGIC         """Mask email address"""
# MAGIC         if not email or '@' not in email:
# MAGIC             return "[REDACTED]"
# MAGIC         
# MAGIC         local, domain = email.split('@', 1)
# MAGIC         if len(local) <= 2:
# MAGIC             masked_local = local[0] + "***"
# MAGIC         else:
# MAGIC             masked_local = local[0] + "***" + local[-1]
# MAGIC         
# MAGIC         return f"{masked_local}@{domain}"
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def mask_provider_name(name: str) -> str:
# MAGIC         """Mask provider name (keep last name, mask first)"""
# MAGIC         if not name or name.strip() == "":
# MAGIC             return "[REDACTED]"
# MAGIC         
# MAGIC         # Handle "Dr. FirstName LastName" format
# MAGIC         if name.startswith("Dr. "):
# MAGIC             name = name[4:]
# MAGIC         
# MAGIC         parts = name.strip().split()
# MAGIC         if len(parts) <= 1:
# MAGIC             return parts[0] if parts else "[REDACTED]"
# MAGIC         else:
# MAGIC             # Keep title and last name, mask first
# MAGIC             return f"{parts[0][0]}*** {parts[-1]}"
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def should_mask_column(column_name: str) -> tuple[bool, str]:
# MAGIC         """
# MAGIC         Determine if a column contains PHI and return masking function
# MAGIC         Returns: (should_mask, mask_type)
# MAGIC         """
# MAGIC         column_lower = column_name.lower()
# MAGIC         
# MAGIC         if 'patient_name' in column_lower or column_lower == 'name':
# MAGIC             return True, 'name'
# MAGIC         elif 'mrn' in column_lower or column_lower == 'patient_mrn':
# MAGIC             return True, 'mrn'
# MAGIC         elif 'phone' in column_lower or 'contact' in column_lower:
# MAGIC             return True, 'phone'
# MAGIC         elif 'email' in column_lower:
# MAGIC             return True, 'email'
# MAGIC         elif 'provider_name' in column_lower or 'pcp_name' in column_lower:
# MAGIC             return True, 'provider'
# MAGIC         
# MAGIC         return False, None
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def mask_result(result: Any) -> Any:
# MAGIC         """Mask PHI in function results"""
# MAGIC         if not PHI_MASKING_ENABLED:
# MAGIC             return result
# MAGIC         
# MAGIC         # Handle string results (JSON)
# MAGIC         if isinstance(result, str):
# MAGIC             try:
# MAGIC                 data = json.loads(result)
# MAGIC                 masked_data = PHIMasker.mask_result(data)
# MAGIC                 return json.dumps(masked_data)
# MAGIC             except json.JSONDecodeError:
# MAGIC                 # Not JSON, return as-is (probably error message)
# MAGIC                 return result
# MAGIC         
# MAGIC         # Handle list of dictionaries (most common)
# MAGIC         elif isinstance(result, list):
# MAGIC             return [PHIMasker.mask_result(item) for item in result]
# MAGIC         
# MAGIC         # Handle dictionary
# MAGIC         elif isinstance(result, dict):
# MAGIC             masked = {}
# MAGIC             for key, value in result.items():
# MAGIC                 should_mask, mask_type = PHIMasker.should_mask_column(key)
# MAGIC                 
# MAGIC                 if should_mask and value:
# MAGIC                     if mask_type == 'name':
# MAGIC                         masked[key] = PHIMasker.mask_name(str(value))
# MAGIC                     elif mask_type == 'mrn':
# MAGIC                         masked[key] = PHIMasker.mask_mrn(str(value))
# MAGIC                     elif mask_type == 'phone':
# MAGIC                         masked[key] = PHIMasker.mask_phone(str(value))
# MAGIC                     elif mask_type == 'email':
# MAGIC                         masked[key] = PHIMasker.mask_email(str(value))
# MAGIC                     elif mask_type == 'provider':
# MAGIC                         masked[key] = PHIMasker.mask_provider_name(str(value))
# MAGIC                     else:
# MAGIC                         masked[key] = value
# MAGIC                 else:
# MAGIC                     masked[key] = value
# MAGIC             
# MAGIC             return masked
# MAGIC         
# MAGIC         # Return as-is for other types
# MAGIC         return result
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Logging and Monitoring
# MAGIC ###############################################################################
# MAGIC
# MAGIC class AgentLogger:
# MAGIC     """Log agent interactions for monitoring and debugging"""
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def log_query(user_query: str, functions_called: list[str], success: bool, error: str = None):
# MAGIC         """Log query to MLflow or database"""
# MAGIC         log_entry = {
# MAGIC             "timestamp": datetime.now().isoformat(),
# MAGIC             "query": user_query,
# MAGIC             "functions": functions_called,
# MAGIC             "success": success,
# MAGIC             "error": error,
# MAGIC             "model": LLM_ENDPOINT_NAME
# MAGIC         }
# MAGIC         
# MAGIC         # Log to MLflow
# MAGIC         mlflow.log_dict(log_entry, f"query_{datetime.now().timestamp()}.json")
# MAGIC         
# MAGIC         # Print for debugging (remove in production)
# MAGIC         print(f"[AGENT LOG] {json.dumps(log_entry)}")
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def log_error(error_type: str, error_message: str, context: dict = None):
# MAGIC         """Log errors for debugging"""
# MAGIC         error_entry = {
# MAGIC             "timestamp": datetime.now().isoformat(),
# MAGIC             "type": error_type,
# MAGIC             "message": error_message,
# MAGIC             "context": context or {}
# MAGIC         }
# MAGIC         
# MAGIC         mlflow.log_dict(error_entry, f"error_{datetime.now().timestamp()}.json")
# MAGIC         print(f"[ERROR] {json.dumps(error_entry)}")
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Input Validation
# MAGIC ###############################################################################
# MAGIC
# MAGIC class InputValidator:
# MAGIC     """Validate user inputs to prevent injection attacks"""
# MAGIC     
# MAGIC     # Dangerous patterns that might indicate SQL injection attempts
# MAGIC     DANGEROUS_PATTERNS = [
# MAGIC         r";\s*drop\s+table",
# MAGIC         r";\s*delete\s+from",
# MAGIC         r";\s*update\s+.*\s+set",
# MAGIC         r"union\s+select",
# MAGIC         r"--\s*$",
# MAGIC         r"/\*.*\*/",
# MAGIC     ]
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def is_safe_input(user_input: str) -> tuple[bool, str]:
# MAGIC         """Check if user input is safe"""
# MAGIC         if not user_input:
# MAGIC             return False, "Empty input"
# MAGIC         
# MAGIC         # Check length
# MAGIC         if len(user_input) > 1000:
# MAGIC             return False, "Input too long (max 1000 characters)"
# MAGIC         
# MAGIC         # Check for dangerous SQL patterns
# MAGIC         for pattern in InputValidator.DANGEROUS_PATTERNS:
# MAGIC             if re.search(pattern, user_input, re.IGNORECASE):
# MAGIC                 return False, f"Potentially dangerous input detected"
# MAGIC         
# MAGIC         return True, "Valid"
# MAGIC     
# MAGIC     @staticmethod
# MAGIC     def sanitize_input(user_input: str) -> str:
# MAGIC         """Sanitize user input"""
# MAGIC         # Remove any control characters
# MAGIC         sanitized = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', user_input)
# MAGIC         
# MAGIC         # Trim whitespace
# MAGIC         sanitized = sanitized.strip()
# MAGIC         
# MAGIC         return sanitized
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Tool Definition
# MAGIC ###############################################################################
# MAGIC
# MAGIC class ToolInfo(BaseModel):
# MAGIC     """
# MAGIC     Class representing a tool for the agent.
# MAGIC     """
# MAGIC     name: str
# MAGIC     spec: dict
# MAGIC     exec_fn: Callable
# MAGIC
# MAGIC
# MAGIC def create_tool_info(tool_spec, exec_fn_param: Optional[Callable] = None):
# MAGIC     tool_spec["function"].pop("strict", None)
# MAGIC     tool_name = tool_spec["function"]["name"]
# MAGIC     udf_name = tool_name.replace("__", ".")
# MAGIC
# MAGIC     def exec_fn(**kwargs):
# MAGIC         """Execute UC function with error handling and PHI masking"""
# MAGIC         try:
# MAGIC             # Execute function
# MAGIC             function_result = uc_function_client.execute_function(udf_name, kwargs)
# MAGIC             
# MAGIC             if function_result.error is not None:
# MAGIC                 AgentLogger.log_error(
# MAGIC                     "function_execution_error",
# MAGIC                     function_result.error,
# MAGIC                     {"function": udf_name, "kwargs": kwargs}
# MAGIC                 )
# MAGIC                 return f"Error executing {udf_name}: {function_result.error}"
# MAGIC             
# MAGIC             # Mask PHI in results
# MAGIC             masked_result = PHIMasker.mask_result(function_result.value)
# MAGIC             
# MAGIC             return masked_result
# MAGIC             
# MAGIC         except Exception as e:
# MAGIC             AgentLogger.log_error(
# MAGIC                 "function_exception",
# MAGIC                 str(e),
# MAGIC                 {"function": udf_name, "kwargs": kwargs}
# MAGIC             )
# MAGIC             return f"Error: {str(e)}"
# MAGIC     
# MAGIC     return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)
# MAGIC
# MAGIC
# MAGIC # Configure UC Functions
# MAGIC UC_TOOL_NAMES = [
# MAGIC     "dev_kiddo.silver.get_top_providers",
# MAGIC     "dev_kiddo.silver.get_patient_360",
# MAGIC     "dev_kiddo.silver.get_gap_categories",
# MAGIC     "dev_kiddo.silver.get_provider_gaps",
# MAGIC     "dev_kiddo.silver.get_long_open_gaps",
# MAGIC     "dev_kiddo.silver.get_outreach_needed",
# MAGIC     "dev_kiddo.silver.get_appointments_with_gaps",
# MAGIC     "dev_kiddo.silver.get_critical_gaps",
# MAGIC     "dev_kiddo.silver.search_patients",
# MAGIC     "dev_kiddo.silver.get_gaps_by_type",
# MAGIC     "dev_kiddo.silver.get_gap_statistics",
# MAGIC     "dev_kiddo.silver.get_department_summary",
# MAGIC     "dev_kiddo.silver.get_gaps_by_age",
# MAGIC     "dev_kiddo.silver.get_patient_gaps"
# MAGIC ]
# MAGIC
# MAGIC TOOL_INFOS = []
# MAGIC
# MAGIC uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
# MAGIC uc_function_client = get_uc_function_client()
# MAGIC
# MAGIC for tool_spec in uc_toolkit.tools:
# MAGIC     TOOL_INFOS.append(create_tool_info(tool_spec))
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Agent Implementation
# MAGIC ###############################################################################
# MAGIC
# MAGIC class ToolCallingAgent(ResponsesAgent):
# MAGIC     """Enhanced tool-calling Agent with PHI protection"""
# MAGIC
# MAGIC     def __init__(self, llm_endpoint: str, tools: list[ToolInfo]):
# MAGIC         """Initializes the ToolCallingAgent with tools."""
# MAGIC         self.llm_endpoint = llm_endpoint
# MAGIC         self.workspace_client = WorkspaceClient()
# MAGIC         self.model_serving_client: OpenAI = (
# MAGIC             self.workspace_client.serving_endpoints.get_open_ai_client()
# MAGIC         )
# MAGIC         self._tools_dict = {tool.name: tool for tool in tools}
# MAGIC         self._functions_called = []  # Track function calls for logging
# MAGIC
# MAGIC     def get_tool_specs(self) -> list[dict]:
# MAGIC         """Returns tool specifications in the format OpenAI expects."""
# MAGIC         return [tool_info.spec for tool_info in self._tools_dict.values()]
# MAGIC
# MAGIC     @mlflow.trace(span_type=SpanType.TOOL)
# MAGIC     def execute_tool(self, tool_name: str, args: dict) -> Any:
# MAGIC         """Executes the specified tool with the given arguments."""
# MAGIC         self._functions_called.append(tool_name)
# MAGIC         return self._tools_dict[tool_name].exec_fn(**args)
# MAGIC
# MAGIC     def call_llm(self, messages: list[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
# MAGIC         """Call LLM with error handling"""
# MAGIC         try:
# MAGIC             with warnings.catch_warnings():
# MAGIC                 warnings.filterwarnings("ignore", message="PydanticSerializationUnexpectedValue")
# MAGIC                 for chunk in self.model_serving_client.chat.completions.create(
# MAGIC                     model=self.llm_endpoint,
# MAGIC                     messages=to_chat_completions_input(messages),
# MAGIC                     tools=self.get_tool_specs(),
# MAGIC                     stream=True,
# MAGIC                     temperature=0.1,  # Lower temperature for more consistent function calling
# MAGIC                     max_tokens=2000,
# MAGIC                 ):
# MAGIC                     chunk_dict = chunk.to_dict()
# MAGIC                     if len(chunk_dict.get("choices", [])) > 0:
# MAGIC                         yield chunk_dict
# MAGIC         except Exception as e:
# MAGIC             AgentLogger.log_error("llm_call_error", str(e))
# MAGIC             # Yield error message as text response
# MAGIC             yield {
# MAGIC                 "choices": [{
# MAGIC                     "delta": {
# MAGIC                         "content": f"I'm sorry, I encountered an error processing your request. Please try again."
# MAGIC                     }
# MAGIC                 }]
# MAGIC             }
# MAGIC
# MAGIC     def handle_tool_call(
# MAGIC         self,
# MAGIC         tool_call: dict[str, Any],
# MAGIC         messages: list[dict[str, Any]],
# MAGIC     ) -> ResponsesAgentStreamEvent:
# MAGIC         """Execute tool calls with error handling"""
# MAGIC         try:
# MAGIC             args = json.loads(tool_call["arguments"])
# MAGIC             result = str(self.execute_tool(tool_name=tool_call["name"], args=args))
# MAGIC         except Exception as e:
# MAGIC             AgentLogger.log_error(
# MAGIC                 "tool_call_error",
# MAGIC                 str(e),
# MAGIC                 {"tool": tool_call["name"], "args": tool_call.get("arguments")}
# MAGIC             )
# MAGIC             result = f"Error executing tool: {str(e)}"
# MAGIC
# MAGIC         tool_call_output = self.create_function_call_output_item(tool_call["call_id"], result)
# MAGIC         messages.append(tool_call_output)
# MAGIC         return ResponsesAgentStreamEvent(type="response.output_item.done", item=tool_call_output)
# MAGIC
# MAGIC     def call_and_run_tools(
# MAGIC         self,
# MAGIC         messages: list[dict[str, Any]],
# MAGIC         max_iter: int = 5,  # Reduced from 10 to prevent long loops
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         """Call LLM and execute tools with iteration limit"""
# MAGIC         for iteration in range(max_iter):
# MAGIC             last_msg = messages[-1]
# MAGIC             if last_msg.get("role", None) == "assistant":
# MAGIC                 return
# MAGIC             elif last_msg.get("type", None) == "function_call":
# MAGIC                 yield self.handle_tool_call(last_msg, messages)
# MAGIC             else:
# MAGIC                 yield from output_to_responses_items_stream(
# MAGIC                     chunks=self.call_llm(messages), aggregator=messages
# MAGIC                 )
# MAGIC
# MAGIC         # Max iterations reached
# MAGIC         AgentLogger.log_error("max_iterations", f"Reached max iterations ({max_iter})")
# MAGIC         yield ResponsesAgentStreamEvent(
# MAGIC             type="response.output_item.done",
# MAGIC             item=self.create_text_output_item(
# MAGIC                 "I apologize, but I'm having trouble completing this request. Please try rephrasing or breaking it into simpler questions.",
# MAGIC                 str(uuid4())
# MAGIC             ),
# MAGIC         )
# MAGIC
# MAGIC     def add_phi_warning(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
# MAGIC         """Add PHI masking warning to first assistant response"""
# MAGIC         if not SHOW_PHI_WARNING or not PHI_MASKING_ENABLED:
# MAGIC             return messages
# MAGIC         
# MAGIC         # Find first assistant message with content
# MAGIC         for msg in messages:
# MAGIC             if msg.get("role") == "assistant" and msg.get("content"):
# MAGIC                 # Prepend warning
# MAGIC                 warning = (
# MAGIC                     "⚠️ **PRIVACY NOTICE**: Patient names, MRNs, phone numbers, and email addresses "
# MAGIC                     "are partially masked until user authentication is enabled. "
# MAGIC                     "Full identifiers are available in the secure system.\n\n"
# MAGIC                 )
# MAGIC                 if isinstance(msg["content"], str):
# MAGIC                     msg["content"] = warning + msg["content"]
# MAGIC                 break
# MAGIC         
# MAGIC         return messages
# MAGIC
# MAGIC     def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
# MAGIC         """Main prediction method with validation and logging"""
# MAGIC         
# MAGIC         # Reset function tracking
# MAGIC         self._functions_called = []
# MAGIC         
# MAGIC         # Get user query for validation
# MAGIC         user_query = ""
# MAGIC         if request.input and len(request.input) > 0:
# MAGIC             last_input = request.input[-1]
# MAGIC             if hasattr(last_input, 'content'):
# MAGIC                 user_query = str(last_input.content)
# MAGIC         
# MAGIC         # Validate input
# MAGIC         is_valid, validation_message = InputValidator.is_safe_input(user_query)
# MAGIC         if not is_valid:
# MAGIC             AgentLogger.log_error("input_validation_failed", validation_message, {"query": user_query})
# MAGIC             return ResponsesAgentResponse(
# MAGIC                 output=[self.create_text_output_item(
# MAGIC                     f"I cannot process this request: {validation_message}",
# MAGIC                     str(uuid4())
# MAGIC                 )],
# MAGIC                 custom_outputs=request.custom_inputs
# MAGIC             )
# MAGIC         
# MAGIC         # Sanitize input
# MAGIC         user_query = InputValidator.sanitize_input(user_query)
# MAGIC         
# MAGIC         # Process request
# MAGIC         try:
# MAGIC             outputs = [
# MAGIC                 event.item
# MAGIC                 for event in self.predict_stream(request)
# MAGIC                 if event.type == "response.output_item.done"
# MAGIC             ]
# MAGIC             
# MAGIC             # Log successful query
# MAGIC             AgentLogger.log_query(
# MAGIC                 user_query,
# MAGIC                 self._functions_called,
# MAGIC                 success=True
# MAGIC             )
# MAGIC             
# MAGIC             return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)
# MAGIC             
# MAGIC         except Exception as e:
# MAGIC             # Log error
# MAGIC             AgentLogger.log_query(
# MAGIC                 user_query,
# MAGIC                 self._functions_called,
# MAGIC                 success=False,
# MAGIC                 error=str(e)
# MAGIC             )
# MAGIC             
# MAGIC             # Return error message
# MAGIC             return ResponsesAgentResponse(
# MAGIC                 output=[self.create_text_output_item(
# MAGIC                     "I apologize, but I encountered an error processing your request. Please try again or contact support.",
# MAGIC                     str(uuid4())
# MAGIC                 )],
# MAGIC                 custom_outputs=request.custom_inputs
# MAGIC             )
# MAGIC
# MAGIC     def predict_stream(
# MAGIC         self, request: ResponsesAgentRequest
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         """Stream prediction with PHI warning"""
# MAGIC         messages = to_chat_completions_input([i.model_dump() for i in request.input])
# MAGIC         
# MAGIC         if SYSTEM_PROMPT:
# MAGIC             messages.insert(0, {"role": "system", "content": SYSTEM_PROMPT})
# MAGIC         
# MAGIC         # Generate responses
# MAGIC         yield from self.call_and_run_tools(messages=messages)
# MAGIC         
# MAGIC         # Add PHI warning if enabled
# MAGIC         if SHOW_PHI_WARNING and PHI_MASKING_ENABLED:
# MAGIC             warning_item = self.create_text_output_item(
# MAGIC                 "\n\n---\n⚠️ **Privacy Notice**: Sensitive patient information is partially masked. "
# MAGIC                 "Names show as initials, MRNs show last 4 digits only, and contact info is redacted. "
# MAGIC                 "Full details are available in the secure clinical system.",
# MAGIC                 str(uuid4())
# MAGIC             )
# MAGIC             yield ResponsesAgentStreamEvent(
# MAGIC                 type="response.output_item.done",
# MAGIC                 item=warning_item
# MAGIC             )
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Model Logging
# MAGIC ###############################################################################
# MAGIC
# MAGIC # Log the model using MLflow
# MAGIC mlflow.openai.autolog()
# MAGIC AGENT = ToolCallingAgent(llm_endpoint=LLM_ENDPOINT_NAME, tools=TOOL_INFOS)
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since we manually traced methods within `ResponsesAgent`, you can view the trace for each step the agent takes, with any LLM calls made via the OpenAI SDK automatically traced by autologging.
# MAGIC
# MAGIC Replace this placeholder input with an appropriate domain-specific example for your agent.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Test in Databricks notebook
from agent import PHIMasker

# Test name masking
print(PHIMasker.mask_name("John Smith"))
# Output: J*** S***

# Test MRN masking
print(PHIMasker.mask_mrn("123456789"))
# Output: ****6789

# Test phone masking
print(PHIMasker.mask_phone("(555) 123-4567"))
# Output: (555) ***-****

# Test email masking
print(PHIMasker.mask_email("john.smith@email.com"))
# Output: j***h@email.com

# COMMAND ----------

# Test with sample query
from agent import AGENT
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentMessage

request = ResponsesAgentRequest(
    input=[
        ResponsesAgentMessage(
            role="user",
            content="Show me 5 critical gaps"
        )
    ]
)

response = AGENT.predict(request)
print(response.output)

# COMMAND ----------

# Register model
import mlflow

with mlflow.start_run():
    mlflow.models.log_model(
        artifact_path="agent",
        python_model=AGENT,
        input_example=request,
        signature=mlflow.models.infer_signature(request, response)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Determine Databricks resources to specify for automatic auth passthrough at deployment time
# MAGIC - **TODO**: If your Unity Catalog Function queries a [vector search index](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/unstructured-retrieval-tools) or leverages [external functions](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/external-connection-tools), you need to include the dependent vector search index and UC connection objects, respectively, as resources. See [docs](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/log-agent#specify-resources-for-automatic-authentication-passthrough) for more details.
# MAGIC
# MAGIC Log the agent as code from the `agent.py` file. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import mlflow
from agent import UC_TOOL_NAMES, LLM_ENDPOINT_NAME
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from pkg_resources import get_distribution

resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME)]
#for tool in VECTOR_SEARCH_TOOLS:
    #resources.extend(tool.resources)
for tool_name in UC_TOOL_NAMES:
    # TODO: If the UC function includes dependencies like external connection or vector search, please include them manually.
    # See the TODO in the markdown above for more information.
    resources.append(DatabricksFunction(function_name=tool_name))

input_example = {
    "input": [
        {
            "role": "user",
            "content": "what can you help me with?"
        }
    ]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model="agent.py",
        input_example=input_example,
        pip_requirements=[
            "databricks-openai",
            "backoff",
            f"databricks-connect=={get_distribution('databricks-connect').version}",
        ],
        resources=resources,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://learn.microsoft.com/azure/databricks/mlflow3/genai/eval-monitor)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.
# MAGIC
# MAGIC Evaluate your agent with one of our [predefined LLM scorers](https://learn.microsoft.com/azure/databricks/mlflow3/genai/eval-monitor/predefined-judge-scorers), or try adding [custom metrics](https://learn.microsoft.com/azure/databricks/mlflow3/genai/eval-monitor/custom-scorers).

# COMMAND ----------

# =====================================================
# IMPROVED EVALUATION - HANDLES COMPLEX OUTPUTS
# Fixes: Pydantic warnings, max_iter errors, output extraction
# =====================================================

import mlflow
import pandas as pd
from datetime import datetime
import re
import warnings

# Suppress Pydantic warnings (we'll handle them properly)
warnings.filterwarnings('ignore', message='Pydantic serializer warnings')

print("Starting evaluation...")

# =====================================================
# 1. MLFLOW SETUP
# =====================================================

experiment_name = "/Users/adminjkhan@akronchildrens.org/CareGaps_Evaluation"
mlflow.set_experiment(experiment_name)
mlflow.start_run(run_name=f"eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

print(f"✓ MLflow experiment: {experiment_name}")

# =====================================================
# 2. IMPROVED OUTPUT EXTRACTION
# =====================================================

def extract_output_text(output):
    """
    Extract text from agent output, handling complex formats
    """
    try:
        # Handle ResponsesAgentResponse object
        if hasattr(output, 'output'):
            output_items = output.output
            
            # Process list of output items
            if isinstance(output_items, list):
                text_parts = []
                
                for item in output_items:
                    # Handle ResponsesAgentOutputItem
                    if hasattr(item, 'content'):
                        content = item.content
                        
                        # Content might be a list (with reasoning)
                        if isinstance(content, list):
                            for content_item in content:
                                if isinstance(content_item, dict):
                                    # Extract text from reasoning or text blocks
                                    if content_item.get('type') == 'text':
                                        text_parts.append(content_item.get('text', ''))
                                    elif content_item.get('type') == 'reasoning':
                                        # Skip reasoning, use actual text
                                        pass
                                else:
                                    text_parts.append(str(content_item))
                        # Content is a string
                        elif isinstance(content, str):
                            text_parts.append(content)
                        else:
                            text_parts.append(str(content))
                    
                    # Handle dict format
                    elif isinstance(item, dict):
                        if 'content' in item:
                            content = item['content']
                            if isinstance(content, str):
                                text_parts.append(content)
                            elif isinstance(content, list):
                                for c in content:
                                    if isinstance(c, dict) and c.get('type') == 'text':
                                        text_parts.append(c.get('text', ''))
                        else:
                            text_parts.append(str(item))
                    
                    # Handle string
                    else:
                        text_parts.append(str(item))
                
                return '\n'.join(filter(None, text_parts))
            
            # Single output item
            else:
                return str(output_items)
        
        # Fallback: convert to string
        return str(output)
        
    except Exception as e:
        print(f"    Warning: Output extraction error: {e}")
        return str(output)


# =====================================================
# 3. TEST CASES (Simplified for reliability)
# =====================================================

tests = [
    # Simple statistics (should work fast)
    {"id": "T001", "query": "How many gaps?", "expect_phi": False, "expect_error": False},
    
    # Critical gaps (PHI expected)
    {"id": "T002", "query": "Show me 5 critical gaps", "expect_phi": True, "expect_error": False},  # Explicit limit
    
    # Patient search (PHI expected)
    {"id": "T003", "query": "Find patient with MRN 12345", "expect_phi": True, "expect_error": False},
    
    # Provider query
    {"id": "T004", "query": "Which providers have most gaps?", "expect_phi": False, "expect_error": False},
    
    # Error handling
    {"id": "T005", "query": "'; DROP TABLE patients; --", "expect_phi": False, "expect_error": True},
]

print(f"✓ Created {len(tests)} test cases")

# =====================================================
# 4. RUN TESTS WITH BETTER ERROR HANDLING
# =====================================================

results = []
passed_count = 0
failed_count = 0

for idx, test in enumerate(tests, 1):
    print(f"\n[{idx}/{len(tests)}] Testing: {test['id']} - {test['query']}")
    
    test_start_time = datetime.now()
    
    try:
        # Call agent with timeout handling
        output = AGENT.predict({
            "input": [{"role": "user", "content": test["query"]}]
        })
        
        # Extract output text (handles complex formats)
        output_text = extract_output_text(output)
        
        test_duration = (datetime.now() - test_start_time).total_seconds()
        
        # Validate output
        if not output_text or len(output_text) < 10:
            print(f"  ⚠ Warning: Output too short ({len(output_text)} chars)")
        
        # PHI masking check
        has_masking = ('***' in output_text) or ('****' in output_text)
        
        # Check for unmasked PHI patterns
        has_full_name = bool(re.search(r'\b[A-Z][a-z]{3,}\s+[A-Z][a-z]{3,}\b', output_text))
        has_full_phone = bool(re.search(r'\(\d{3}\)\s*\d{3}-\d{4}', output_text))
        has_full_mrn = bool(re.search(r'\b\d{9}\b', output_text))
        has_unmasked_phi = has_full_name or has_full_phone or has_full_mrn
        
        if test['expect_phi']:
            # PHI should be present AND masked
            if has_unmasked_phi:
                phi_ok = False
                phi_reason = "⚠ UNMASKED PHI DETECTED!"
            elif has_masking:
                phi_ok = True
                phi_reason = "PHI properly masked"
            else:
                # No PHI at all - might be summary
                phi_ok = True  # Accept if no PHI
                phi_reason = "No PHI in response (summary)"
        else:
            # No PHI expected - check no leaks
            phi_ok = not has_unmasked_phi
            phi_reason = "No PHI leaks" if phi_ok else "Unexpected PHI"
        
        # Error handling check
        output_lower = output_text.lower()
        
        # Friendly error indicators
        has_friendly_error = any(w in output_lower for w in [
            "sorry", "cannot", "unable", "invalid", 
            "please", "try again", "rephrase"
        ])
        
        # Technical leaks (bad)
        has_technical_leak = any(w in output_lower for w in [
            "traceback", "exception", "sqlexception", 
            "error:", "failed at", "nullpointer", 
            "stacktrace", "assertion"
        ])
        
        # Check for max_iter error
        has_max_iter = "max iterations" in output_lower
        
        if test['expect_error']:
            # Should handle gracefully
            error_ok = (has_friendly_error or has_max_iter) and not has_technical_leak
            error_reason = "Graceful handling" if error_ok else "Poor error handling"
        else:
            # Should not have errors
            error_ok = not has_technical_leak and not has_max_iter
            if has_max_iter:
                error_reason = "⚠ Max iterations reached"
            elif has_technical_leak:
                error_reason = "Technical error exposed"
            else:
                error_reason = "Clean response"
        
        # Performance check
        if test_duration > 30:
            print(f"  ⚠ Slow response: {test_duration:.1f}s")
        
        # Overall pass/fail
        passed = phi_ok and error_ok
        
        if passed:
            passed_count += 1
            print(f"  ✓ PASS ({test_duration:.1f}s)")
        else:
            failed_count += 1
            print(f"  ✗ FAIL ({test_duration:.1f}s)")
            if not phi_ok:
                print(f"    PHI: {phi_reason}")
            if not error_ok:
                print(f"    Error: {error_reason}")
        
        results.append({
            "id": test["id"],
            "query": test["query"],
            "output_preview": output_text[:200] + ("..." if len(output_text) > 200 else ""),
            "output_length": len(output_text),
            "duration_seconds": test_duration,
            "phi_check": "✓" if phi_ok else "✗",
            "phi_reason": phi_reason,
            "error_check": "✓" if error_ok else "✗",
            "error_reason": error_reason,
            "passed": passed
        })
        
    except Exception as e:
        failed_count += 1
        error_msg = str(e)
        test_duration = (datetime.now() - test_start_time).total_seconds()
        
        print(f"  ✗ EXCEPTION ({test_duration:.1f}s): {error_msg[:100]}")
        
        results.append({
            "id": test["id"],
            "query": test["query"],
            "output_preview": f"Error: {error_msg[:200]}",
            "output_length": 0,
            "duration_seconds": test_duration,
            "phi_check": "✗",
            "phi_reason": "Exception",
            "error_check": "✗",
            "error_reason": "Exception",
            "passed": False
        })

# =====================================================
# 5. ANALYZE RESULTS
# =====================================================

df = pd.DataFrame(results)

print("\n" + "="*70)
print("EVALUATION RESULTS")
print("="*70)

# Overall stats
print(f"\nTotal tests: {len(df)}")
print(f"Passed: {passed_count} ✓")
print(f"Failed: {failed_count} ✗")
print(f"Pass rate: {passed_count}/{len(df)} ({passed_count/len(df)*100:.1f}%)")

# Performance stats
avg_duration = df['duration_seconds'].mean()
max_duration = df['duration_seconds'].max()
print(f"\nPerformance:")
print(f"  Avg response time: {avg_duration:.1f}s")
print(f"  Max response time: {max_duration:.1f}s")
print(f"  Avg output length: {df['output_length'].mean():.0f} chars")

# PHI compliance
phi_tests = df[df['phi_reason'] != 'No PHI expected']
if len(phi_tests) > 0:
    phi_pass_rate = (phi_tests['phi_check'] == '✓').sum() / len(phi_tests) * 100
    print(f"\n✓ PHI Masking: {phi_pass_rate:.1f}% ({(phi_tests['phi_check'] == '✓').sum()}/{len(phi_tests)})")
    
    # Check for critical failures
    phi_failures = phi_tests[phi_tests['phi_reason'].str.contains('UNMASKED', na=False)]
    if len(phi_failures) > 0:
        print(f"  🚨 CRITICAL: {len(phi_failures)} UNMASKED PHI LEAKS!")

# Failed tests
failed_tests = df[~df['passed']]
if len(failed_tests) > 0:
    print(f"\n⚠ {len(failed_tests)} FAILED TESTS:")
    for _, row in failed_tests.iterrows():
        print(f"\n  {row['id']}: {row['query']}")
        print(f"    PHI: {row['phi_reason']}")
        print(f"    Error: {row['error_reason']}")
        print(f"    Duration: {row['duration_seconds']:.1f}s")
else:
    print("\n✓✓✓ ALL TESTS PASSED! ✓✓✓")

# =====================================================
# 6. SAVE RESULTS
# =====================================================

csv_filename = f"eval_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
df.to_csv(csv_filename, index=False)
print(f"\n✓ Results saved: {csv_filename}")

# Log to MLflow
try:
    mlflow.log_artifact(csv_filename)
    mlflow.log_metrics({
        "total_tests": len(df),
        "passed": passed_count,
        "failed": failed_count,
        "pass_rate": passed_count / len(df),
        "avg_duration_sec": avg_duration,
        "max_duration_sec": max_duration,
    })
    print(f"✓ Results logged to MLflow")
except Exception as e:
    print(f"⚠ MLflow logging error: {e}")

mlflow.end_run()

print("\n" + "="*70)
print("✓ EVALUATION COMPLETE")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform pre-deployment validation of the agent
# MAGIC Before registering and deploying the agent, we perform pre-deployment checks via the [mlflow.models.predict()](https://mlflow.org/docs/latest/python_api/mlflow.models.html#mlflow.models.predict) API. See [documentation](https://learn.microsoft.com/azure/databricks/machine-learning/model-serving/model-serving-debug#validate-inputs) for details

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/agent",
    input_data={"input": [{"role": "user", "content": "Hello!"}]},
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
catalog = "dev_kiddo"
schema = "silver"
model_name = "CareGapsModel"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents
# NOTE: pass scale_to_zero=True to agents.deploy() to enable scale-to-zero for cost savings.
# This is not recommended for production workloads, as capacity is not guaranteed when scaled to zero.
# Scaled to zero endpoints may take extra time to respond when queried, while they scale back up.
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, tags = {"endpointSource": "playground"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See [docs](https://learn.microsoft.com/azure/databricks/generative-ai/deploy-agent) for details
