import { tool } from 'ai';
import { z } from 'zod';

/**
 * Databricks tool call ID used by Agent Serving endpoints.
 * When a Databricks agent returns a tool call, it uses this identifier.
 */
export const DATABRICKS_TOOL_CALL_ID = 'databricks_tool_call';

/**
 * Tool definition that passes through Databricks agent tool calls.
 * This is a no-op tool that lets the AI SDK recognize and render
 * Databricks-specific tool calls in the UI.
 */
export const DATABRICKS_TOOL_DEFINITION = tool({
  description:
    'Tool call made by the Databricks agent. This is handled by the serving endpoint.',
  parameters: z.object({}).passthrough(),
});
