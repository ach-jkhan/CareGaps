import { getDatabricksToken, getCachedCliHost } from '@chat-template/auth';
import { getHostUrl } from '@chat-template/utils';

/**
 * Get the Databricks workspace URL, preferring the cached CLI host
 * when DATABRICKS_HOST isn't set (CLI auth mode).
 */
function resolveHostUrl(): string {
  const cachedHost = getCachedCliHost();
  if (cachedHost) {
    return cachedHost;
  }
  return getHostUrl();
}

/**
 * Execute a SQL statement against a Databricks SQL Warehouse
 * via the SQL Statements API.
 *
 * @see https://docs.databricks.com/api/workspace/statementexecution
 */
export async function executeSql<
  T extends Record<string, unknown> = Record<string, unknown>,
>(statement: string): Promise<T[]> {
  const warehouseId = process.env.DATABRICKS_SQL_WAREHOUSE_ID;
  if (!warehouseId) {
    throw new Error(
      'DATABRICKS_SQL_WAREHOUSE_ID is not set. ' +
        'Create a SQL Warehouse in Databricks and add its ID to .env',
    );
  }

  const token = await getDatabricksToken();
  const hostUrl = resolveHostUrl();

  const response = await fetch(`${hostUrl}/api/2.0/sql/statements/`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      warehouse_id: warehouseId,
      statement,
      wait_timeout: '30s',
      disposition: 'INLINE',
      format: 'JSON_ARRAY',
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Databricks SQL API error (${response.status}): ${text}`);
  }

  const data = await response.json();

  if (data.status?.state === 'FAILED') {
    const msg = data.status?.error?.message ?? 'Unknown SQL execution error';
    throw new Error(`SQL execution failed: ${msg}`);
  }

  if (data.status?.state !== 'SUCCEEDED') {
    throw new Error(
      `SQL statement did not complete. State: ${data.status?.state}`,
    );
  }

  // Parse the column + data_array format into objects
  const columns: { name: string }[] = data.manifest?.schema?.columns ?? [];
  const rows: unknown[][] = data.result?.data_array ?? [];

  return rows.map((row) => {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < columns.length; i++) {
      obj[columns[i].name] = row[i];
    }
    return obj as T;
  });
}
