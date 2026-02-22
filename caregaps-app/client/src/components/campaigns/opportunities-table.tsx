import { useState } from 'react';
import {
  Table,
  TableBody,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import type { FluOpportunity } from '@/lib/campaign-types';
import { OpportunityRow } from './opportunity-row';

interface OpportunitiesTableProps {
  opportunities: FluOpportunity[];
  onApprove: (id: string) => void;
  onSend: (id: string) => void;
}

export function OpportunitiesTable({
  opportunities,
  onApprove,
  onSend,
}: OpportunitiesTableProps) {
  const [expandedId, setExpandedId] = useState<string | null>(null);

  if (opportunities.length === 0) {
    return (
      <div className="flex h-32 items-center justify-center rounded-md border text-muted-foreground">
        No opportunities found
      </div>
    );
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Sibling MRN</TableHead>
          <TableHead>Sibling</TableHead>
          <TableHead>Scheduled Patient</TableHead>
          <TableHead>Appt Date</TableHead>
          <TableHead>Location</TableHead>
          <TableHead>Flags</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Actions</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {opportunities.map((opp) => (
          <OpportunityRow
            key={opp.id}
            opportunity={opp}
            isExpanded={expandedId === opp.id}
            onToggleExpand={() =>
              setExpandedId(expandedId === opp.id ? null : opp.id)
            }
            onApprove={onApprove}
            onSend={onSend}
          />
        ))}
      </TableBody>
    </Table>
  );
}
