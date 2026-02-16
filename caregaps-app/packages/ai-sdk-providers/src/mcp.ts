/**
 * Extract approval status from MCP tool call output.
 * Returns true if approved, false if denied, undefined if not an approval response.
 */
export function extractApprovalStatus(
  output: unknown,
): boolean | undefined {
  if (!output || typeof output !== 'object') {
    return undefined;
  }

  const obj = output as Record<string, unknown>;

  // Check for explicit approval/denial fields
  if ('approved' in obj && typeof obj.approved === 'boolean') {
    return obj.approved;
  }

  if ('status' in obj && typeof obj.status === 'string') {
    if (obj.status === 'approved') return true;
    if (obj.status === 'denied' || obj.status === 'rejected') return false;
  }

  return undefined;
}
