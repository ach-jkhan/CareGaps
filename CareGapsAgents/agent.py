
import json
import os
import re
from typing import Any, Callable, Generator, Optional
from uuid import uuid4
import warnings
from datetime import datetime

import mlflow
import openai
from databricks.sdk import WorkspaceClient
from databricks_openai import UCFunctionToolkit, VectorSearchRetrieverTool
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)
from openai import OpenAI
from pydantic import BaseModel
from unitycatalog.ai.core.base import get_uc_function_client


############################################
# Configuration
############################################
#LLM_ENDPOINT_NAME = "databricks-gpt-oss-20b"
LLM_ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"

# Data Mode: "real" = force function calls for live data, "demo" = allow synthetic example data
# Set via environment variable or change here. Use "demo" for stakeholder presentations, "real" for production.
DATA_MODE = os.environ.get("CAREGAPS_DATA_MODE", "real")

# System Prompt - Example-Driven for Llama 3.3 70B
_BASE_PROMPT = """You are the CareGaps Assistant for Akron Children's Hospital. Your role is to help clinicians, care coordinators, and administrators query and analyze patient care gaps AND outreach campaigns using natural language.

CAPABILITIES:
You have access to 19 SQL functions:

**Care Gaps Analysis (15 functions):**
- Patient-specific queries (search, view gaps, 360-degree view)
- Priority and urgency queries (critical gaps, long-open gaps, outreach needs, no appointments)
- Provider and department analysis
- Statistical overviews and trends
- Appointment coordination
- Gap type and category analysis

**Campaign Analytics (4 functions):**
- Campaign statistics and metrics
- Search campaign opportunities by patient, location, or MRN
- List and filter campaign opportunities
- Patient campaign history

DATA SCOPE:
- Pediatric patients with active care gaps
- Gap types: Immunizations, Well Child Visits, BMI Screenings, Developmental Assessments, etc.
- Priority levels: Critical, Important, Routine
- Provider assignments and departments
- Appointment scheduling information
- Patient contact information (phone, email)
- **Flu Vaccine Piggybacking Campaign:** Identifies siblings who need flu vaccines and can piggyback on a household member's existing appointment

CAMPAIGN CONTEXT — FLU VACCINE PIGGYBACKING:
This is an agentic AI campaign that identifies TRUE piggybacking opportunities:
- A "subject patient" has an upcoming appointment
- A sibling in the same household is overdue for their flu vaccine but has NO appointment of their own
- The system suggests: "Bring sibling for their flu shot while you're here for the appointment"
- Siblings who already have their own appointments are EXCLUDED (this is the AI differentiator)
- Campaign types: FLU_VACCINE (active), LAB_PIGGYBACKING and DEPRESSION_SCREENING (coming soon)
- Statuses: pending → approved → sent → completed

IMPORTANT — CHAT vs DASHBOARD BOUNDARY:
This chat agent handles ANALYTICAL and READ-ONLY queries only.
Campaign operations (approve, send messages, change status) belong in the **Flu Campaign Dashboard**.
If a user asks to "send a message", "approve this opportunity", or "mark as completed":
→ Respond: "That action is available in the Campaign Dashboard. Navigate to **Campaigns → Flu Vaccine** in the sidebar to review, approve, and send messages."

SCOPE BOUNDARY:
You ONLY answer questions related to pediatric care gaps, patient outreach, campaigns, flu vaccine piggybacking, and Akron Children's Hospital clinical operations.
If a user asks about anything unrelated (recipes, general knowledge, coding, weather, etc.), politely decline:
→ "I'm the CareGaps Assistant and can only help with care gap analysis, outreach campaigns, and patient data for Akron Children's Hospital. How can I help you with care gaps today?"

RESPONSE GUIDELINES:
1. ALWAYS call a function to get data before responding — never make up data
2. Format results as markdown tables with | separators
3. ALWAYS include "### Next Best Actions:" section using bullet points (•)
4. Show ALL rows returned — never truncate results
5. Prioritize critical gaps over routine ones
6. Suggest relevant follow-up questions
7. Be concise but complete

EXAMPLE INTERACTIONS:

User: "Show me critical gaps"
You: [Call get_critical_gaps(limit_rows=100)]
     Present the returned data as a table, then add:
     ### Next Best Actions:
     • Patients with no upcoming appointments need priority outreach
     • Gaps open >90 days should be escalated
     • Consider group vaccination clinic for immunization gaps

User: "How is the flu campaign going?"
You: [Call get_campaign_statistics(campaign_type_filter='FLU_VACCINE')]
     Present the returned metrics as a table, then add:
     ### Next Best Actions:
     • Opportunities still pending review — head to the Campaign Dashboard to approve
     • Asthma patients should be prioritized (higher flu risk)
     • Focus on HIGH confidence matches first for best outreach ROI

User: "Show flu opportunities at Beachwood"
You: [Call get_campaign_opportunities(campaign_type_filter='FLU_VACCINE', status_filter='', location_filter='Beachwood', limit_rows=50)]
     Present the returned data as a table, then add:
     ### Next Best Actions:
     • Review and approve these in the Campaign Dashboard
     • Prioritize asthma patients for outreach
     • Check if any siblings share the same appointment date for batch processing

User: "Send a message to this patient"
You: "That action is available in the Campaign Dashboard. Navigate to **Campaigns → Flu Vaccine** in the sidebar to review, approve, and send messages."

User: "Find patient John Smith"
You: [Call search_patients(search_term='John Smith')]
     Present results as a table, suggest get_patient_360() for more detail.

User: "Any asthma siblings in the flu campaign?"
You: [Call get_campaign_opportunities(campaign_type_filter='FLU_VACCINE', status_filter='', location_filter='', limit_rows=100)]
     Filter and highlight rows where has_asthma = 'Y', recommend prioritizing these for outreach.

FUNCTION SELECTION (19 functions):

**Care Gaps (15):**
- Patient search/find → search_patients()
- Patient gaps → get_patient_gaps()
- Comprehensive/360/everything about patient → get_patient_360()
- Critical/urgent gaps → get_critical_gaps()
- Long-open gaps → get_long_open_gaps()
- Outreach needed → get_outreach_needed()
- Gaps with NO appointments → get_gaps_no_appointments()
- Provider/department gaps → get_provider_gaps()
- Department summary → get_department_summary()
- Top providers → get_top_providers()
- Gap statistics → get_gap_statistics()
- Gaps by type → get_gaps_by_type()
- Gaps by age → get_gaps_by_age()
- Gap categories → get_gap_categories()
- Appointments with gaps → get_appointments_with_gaps()

**Campaigns (4):**
- Campaign stats/metrics/overview → get_campaign_statistics(campaign_type_filter)
- Search by MRN/name/location → search_campaign_opportunities(search_term, campaign_type_filter)
- List/filter opportunities → get_campaign_opportunities(campaign_type_filter, status_filter, location_filter, limit_rows)
- Patient campaign history → get_patient_campaign_history(patient_mrn_filter)

CAMPAIGN TYPE VALUES:
- "FLU_VACCINE" — Flu vaccine piggybacking (active)
- "LAB_PIGGYBACKING" — Lab piggybacking (coming soon)
- "DEPRESSION_SCREENING" — Depression screening PHQ-9 (coming soon)

When user mentions "flu", "flu vaccine", "flu campaign", "piggybacking" → use campaign_type_filter = "FLU_VACCINE"

CONTEXT MAINTENANCE:
- Remember conversation history
- When user says "this patient" or "that patient", refer to the most recently mentioned patient
- When user asks for "more information" about a patient just shown, use get_patient_360() with that patient's ID

CRITICAL:
- ALWAYS call a function to get data — never fabricate patient names, MRNs, or numbers
- ALWAYS format results as markdown tables with | separators
- NEVER return raw comma-separated data
- ALWAYS include "### Next Best Actions:" section with bullet points (•) after data
- SHOW ALL ROWS — never truncate to 3 or 10 results
- For campaign operations (approve, send, update status) → redirect to Campaign Dashboard"""

# Demo mode adds a fallback for when functions return empty results
_DEMO_SUFFIX = """

DEMO MODE: If a function returns empty results or is unavailable, you may generate realistic sample data to demonstrate the system's capabilities. Use plausible patient names, MRNs, dates, and statistics for a pediatric hospital setting."""

SYSTEM_PROMPT = _BASE_PROMPT + (_DEMO_SUFFIX if DATA_MODE == "demo" else "")
print(f"[CONFIG] Data mode: {DATA_MODE}, prompt length: {len(SYSTEM_PROMPT)} chars")


###############################################################################
## Logging and Monitoring
###############################################################################

class AgentLogger:
    """Log agent interactions for monitoring and debugging"""

    @staticmethod
    def log_query(user_query: str, functions_called: list[str], success: bool, error: str = None):
        """Log query to MLflow or database"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "query": user_query,
            "functions": functions_called,
            "success": success,
            "error": error,
            "model": LLM_ENDPOINT_NAME
        }

        # Log to MLflow
        mlflow.log_dict(log_entry, f"query_{datetime.now().timestamp()}.json")

        # Print for debugging (remove in production)
        print(f"[AGENT LOG] {json.dumps(log_entry)}")

    @staticmethod
    def log_error(error_type: str, error_message: str, context: dict = None):
        """Log errors for debugging"""
        error_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": error_type,
            "message": error_message,
            "context": context or {}
        }

        mlflow.log_dict(error_entry, f"error_{datetime.now().timestamp()}.json")
        print(f"[ERROR] {json.dumps(error_entry)}")


###############################################################################
## Input Validation
###############################################################################

class InputValidator:
    """Validate user inputs to prevent injection attacks"""

    # Dangerous patterns that might indicate SQL injection attempts
    DANGEROUS_PATTERNS = [
        r";\s*drop\s+table",
        r";\s*delete\s+from",
        r";\s*update\s+.*\s+set",
        r"union\s+select",
        r"--\s*$",
        r"/\*.*\*/",
    ]

    @staticmethod
    def is_safe_input(user_input: str) -> tuple[bool, str]:
        """Check if user input is safe"""
        if not user_input:
            return False, "Empty input"

        # Check length
        if len(user_input) > 1000:
            return False, "Input too long (max 1000 characters)"

        # Check for dangerous SQL patterns
        for pattern in InputValidator.DANGEROUS_PATTERNS:
            if re.search(pattern, user_input, re.IGNORECASE):
                return False, f"Potentially dangerous input detected"

        return True, "Valid"

    @staticmethod
    def sanitize_input(user_input: str) -> str:
        """Sanitize user input"""
        # Remove any control characters
        sanitized = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', user_input)

        # Trim whitespace
        sanitized = sanitized.strip()

        return sanitized


###############################################################################
## Tool Definition
###############################################################################

class ToolInfo(BaseModel):
    """
    Class representing a tool for the agent.
    """
    name: str
    spec: dict
    exec_fn: Callable


def create_tool_info(tool_spec, exec_fn_param: Optional[Callable] = None):
    tool_spec["function"].pop("strict", None)
    tool_name = tool_spec["function"]["name"]
    udf_name = tool_name.replace("__", ".")

    def exec_fn(**kwargs):
        """Execute UC function with error handling and PHI masking"""
        try:
            # Execute function
            function_result = uc_function_client.execute_function(udf_name, kwargs)

            if function_result.error is not None:
                AgentLogger.log_error(
                    "function_execution_error",
                    function_result.error,
                    {"function": udf_name, "kwargs": kwargs}
                )
                return f"Error executing {udf_name}: {function_result.error}"

            return function_result.value

        except Exception as e:
            AgentLogger.log_error(
                "function_exception",
                str(e),
                {"function": udf_name, "kwargs": kwargs}
            )
            return f"Error: {str(e)}"

    return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)


# Configure UC Functions
UC_TOOL_NAMES = [
    # Care Gaps (15 functions)
    "dev_kiddo.silver.get_top_providers",
    "dev_kiddo.silver.get_patient_360",
    "dev_kiddo.silver.get_gap_categories",
    "dev_kiddo.silver.get_provider_gaps",
    "dev_kiddo.silver.get_long_open_gaps",
    "dev_kiddo.silver.get_outreach_needed",
    "dev_kiddo.silver.get_appointments_with_gaps",
    "dev_kiddo.silver.get_critical_gaps",
    "dev_kiddo.silver.search_patients",
    "dev_kiddo.silver.get_gaps_by_type",
    "dev_kiddo.silver.get_gap_statistics",
    "dev_kiddo.silver.get_department_summary",
    "dev_kiddo.silver.get_gaps_by_age",
    "dev_kiddo.silver.get_gaps_no_appointments",
    "dev_kiddo.silver.get_patient_gaps",
    # Campaign Analytics (4 functions)
    "dev_kiddo.silver.get_campaign_statistics",
    "dev_kiddo.silver.search_campaign_opportunities",
    "dev_kiddo.silver.get_campaign_opportunities",
    "dev_kiddo.silver.get_patient_campaign_history",
]

TOOL_INFOS = []
uc_function_client = None

try:
    uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
    uc_function_client = get_uc_function_client()

    for tool_spec in uc_toolkit.tools:
        TOOL_INFOS.append(create_tool_info(tool_spec))
except Exception as e:
    print(f"[INIT] UC toolkit unavailable (expected during model logging): {e}")


###############################################################################
## Agent Implementation
###############################################################################

class ToolCallingAgent(ResponsesAgent):
    """Enhanced tool-calling Agent with PHI protection"""

    def __init__(self, llm_endpoint: str, tools: list[ToolInfo]):
        """Initializes the ToolCallingAgent with tools."""
        self.llm_endpoint = llm_endpoint
        try:
            self.workspace_client = WorkspaceClient()
            self.model_serving_client: OpenAI = (
                self.workspace_client.serving_endpoints.get_open_ai_client()
            )
        except Exception as e:
            print(f"[INIT] WorkspaceClient unavailable (expected during model logging): {e}")
            self.workspace_client = None
            self.model_serving_client = None
        self._tools_dict = {tool.name: tool for tool in tools}
        self._functions_called = []  # Track function calls for logging

    def get_tool_specs(self) -> list[dict]:
        """Returns tool specifications in the format OpenAI expects."""
        return [tool_info.spec for tool_info in self._tools_dict.values()]

    @mlflow.trace(span_type=SpanType.TOOL)
    def execute_tool(self, tool_name: str, args: dict) -> Any:
        """Executes the specified tool with the given arguments."""
        self._functions_called.append(tool_name)

        # Execute the tool
        result = self._tools_dict[tool_name].exec_fn(**args)

         # ⭐ Format results instead of returning raw
        if isinstance(result, dict):
            formatted = self._format_dict_result(result)
        elif isinstance(result, list):
            formatted = self._format_list_result(result)
        else:
            formatted = str(result)

        # ✅ Add instruction for LLM to provide next steps
        # Apply to both lists (patient data) AND dicts (statistics)
        if isinstance(result, (list, dict)) and result:
            formatted += "\n\n[INSTRUCTION: After presenting this data, you MUST add a '### Next Best Actions:' section with 3-5 specific, actionable recommendations based on this data. Be concrete and clinical in your recommendations.]"

        return formatted

    def call_llm(self, messages: list[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
        """Call LLM with error handling"""
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="PydanticSerializationUnexpectedValue")
                for chunk in self.model_serving_client.chat.completions.create(
                    model=self.llm_endpoint,
                    messages=to_chat_completions_input(messages),
                    tools=self.get_tool_specs(),
                    stream=True,
                    temperature=0.0,  # Lower temperature for more consistent function calling
                    max_tokens=4096,
                ):
                    chunk_dict = chunk.to_dict()
                    if len(chunk_dict.get("choices", [])) > 0:
                        yield chunk_dict
        except Exception as e:
            error_msg = str(e)

            AgentLogger.log_error("llm_call_error", error_msg)
            # Yield error message as text response
            yield {
                "choices": [{
                    "delta": {
                        "content": f"I'm sorry, I encountered an error processing your request. Please try again."
                    }
                }]
            }

    def handle_tool_call(
        self,
        tool_call: dict[str, Any],
        messages: list[dict[str, Any]],
    ) -> ResponsesAgentStreamEvent:
        """Execute tool calls with error handling"""
        try:
            raw_name = tool_call["name"]
            clean_name = self._sanitize_function_name(raw_name)

            args = json.loads(tool_call["arguments"])

            if isinstance(args, dict):
                # Remove empty keys (LLM sometimes generates {"": ""})
                args = {k: v for k, v in args.items() if k and k.strip()}

            # ADD THIS: If args is now empty dict, check if function needs params
            if not args:
                # Check if function has required parameters
                tool_info = self._tools_dict.get(clean_name)
                if tool_info and hasattr(tool_info, 'parameters'):
                    # If function has required params but we have none, that's an error
                    required_params = getattr(tool_info.parameters, 'required', [])
                    if required_params:
                        print(f"[ERROR] Function '{clean_name}' requires params: {required_params}")
                        result = f"Error: This function requires parameters. Please provide: {', '.join(required_params)}"
                        # Skip to the end
                        tool_call_output = self.create_function_call_output_item(tool_call["call_id"], result)
                        messages.append(tool_call_output)
                        return ResponsesAgentStreamEvent(type="response.output_item.done", item=tool_call_output)

            if clean_name not in self._tools_dict:
                print(f"[ERROR] Function '{clean_name}' not found.")
                print(f"[Error] Available: {list(self._tools_dict.keys())[:3]}...")
                result = f"Error: Function not found. Please rephrase your query."
            else:
                result = str(self.execute_tool(tool_name=clean_name, args=args))


        except Exception as e:
            AgentLogger.log_error(
                "tool_call_error",
                str(e),
                {"tool": tool_call["name"], "args": tool_call.get("arguments")}
            )
            result = f"Error executing tool: {str(e)}"

        tool_call_output = self.create_function_call_output_item(tool_call["call_id"], result)
        messages.append(tool_call_output)
        return ResponsesAgentStreamEvent(type="response.output_item.done", item=tool_call_output)

    def call_and_run_tools(
    self,
    messages: list[dict[str, Any]],
    max_iter: int = 10,  # ⭐ Increased back to 10
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        """Call LLM and execute tools with iteration limit"""

        # ⭐ ADD THIS: Limit conversation history to prevent context overflow
        if len(messages) > 7:
            system_prompt = messages[0] if messages[0].get('role') == 'system' else None
            recent_messages = messages[-6:]

            if system_prompt:
                messages = [system_prompt] + recent_messages
            else:
                messages = recent_messages

            print(f"[Debug] Trimmed to {len(messages)} messages")

        # Continue with existing loop
        for iteration in range(max_iter):
            last_msg = messages[-1]
            if last_msg.get("role", None) == "assistant":
                return
            elif last_msg.get("type", None) == "function_call":
                yield self.handle_tool_call(last_msg, messages)
            else:
                yield from output_to_responses_items_stream(
                    chunks=self.call_llm(messages), aggregator=messages
                )

        # Max iterations reached
        AgentLogger.log_error("max_iterations", f"Reached max iterations ({max_iter})")
        yield ResponsesAgentStreamEvent(
            type="response.output_item.done",
            item=self.create_text_output_item(
                "I apologize, but I'm having trouble completing this request. Please try rephrasing or breaking it into simpler questions.",
                str(uuid4())
            ),
        )

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """Generate a response for the given request"""

        # Generate response using predict_stream
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]

        # Handle custom_inputs for both formats
        custom_outputs = None
        if isinstance(request, dict):
            custom_outputs = request.get('custom_inputs', None)
        elif hasattr(request, 'custom_inputs'):
            custom_outputs = request.custom_inputs

        return ResponsesAgentResponse(output=outputs, custom_outputs=custom_outputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        """Stream prediction with PHI warning"""

        # ⭐ Handle both dict and ResponsesAgentRequest formats
        if isinstance(request, dict):
            # Dict format
            messages = request.get('input', [])
        elif hasattr(request, 'input'):
            # ResponsesAgentRequest format
            if hasattr(request.input[0], 'model_dump'):
                messages = to_chat_completions_input([i.model_dump() for i in request.input])
            else:
                messages = to_chat_completions_input(request.input)
        else:
            messages = []

        if SYSTEM_PROMPT:
            messages.insert(0, {"role": "system", "content": SYSTEM_PROMPT})

        # Generate responses
        yield from self.call_and_run_tools(messages=messages)

    def _call_agent(self, request: ResponsesAgentRequest) -> Generator:
        """Internal method to call agent with proper message handling"""
        messages = to_chat_completions_input([i.model_dump() for i in request.input])

        if SYSTEM_PROMPT:
            messages.insert(0, {"role": "system", "content": SYSTEM_PROMPT})

        yield from self.call_and_run_tools(messages=messages)

    def _format_dict_result(self, result: dict) -> str:
        """Format dictionary result as readable text"""
        lines = []
        for key, value in result.items():
            readable_key = key.replace('_', ' ').title()
            lines.append(f"{readable_key}: {value}")
        return "\n".join(lines)

    def _format_list_result(self, result: list) -> str:
        """Format list result as table or bullets"""
        if not result:
            return "No results found."

        if isinstance(result[0], dict):
            return self._format_table(result)
        else:
            return "\n".join(f"• {item}" for item in result)


    def _format_table(self, data: list) -> str:
        """Format list of dicts as a markdown table"""
        if not data:
            return "No results found."

        headers = list(data[0].keys())
        readable_headers = [h.replace('_', ' ').title() for h in headers]

        lines = []
        lines.append("| " + " | ".join(readable_headers) + " |")  # Proper markdown
        lines.append("|" + "|".join(["---" for _ in headers]) + "|")  # Proper separator

        for row in data:
            # Truncate long cell values to 80 chars to keep tables readable
            values = [str(row.get(h, ''))[:80] for h in headers]
            lines.append("| " + " | ".join(values) + " |")

        # Add total count
        lines.append(f"\n**Total: {len(data)} results**")
        lines.append("\n### Next Best Actions:")
        lines.append("Please provide 3-5 specific action items based on this data.")

        return "\n".join(lines)

    def _sanitize_function_name(self, raw_name: str) -> str:
        """
        Remove hallucinated tokens from function names.
        Fixes: dev_kiddo__silver__get_statistics<|channel|>commentary
        """
        if not raw_name:
            return raw_name

        # Known hallucination tokens
        bad_tokens = [
            '<|channel|>',
            '<|commentary|>',
            'commentary',
            'channel',
            '<|',
            '|>',
        ]

        sanitized = raw_name
        for token in bad_tokens:
            sanitized = sanitized.replace(token, '')

        # Log if we had to clean
        if sanitized != raw_name:
            print(f"[SANITIZED] {raw_name} → {sanitized}")

        return sanitized

###############################################################################
## Model Logging
###############################################################################

# autolog is optional — isolate it so a failure doesn't discard tools
try:
    mlflow.openai.autolog(disable=True)
except Exception as e:
    print(f"[INIT] autolog skipped (safe to ignore): {e}")

# Create the agent with whatever tools were loaded (may be empty during model logging)
print(f"[INIT] Creating agent with {len(TOOL_INFOS)} tools")
AGENT = ToolCallingAgent(llm_endpoint=LLM_ENDPOINT_NAME, tools=TOOL_INFOS)
mlflow.models.set_model(AGENT)
