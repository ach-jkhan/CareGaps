import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { TableCell, TableRow } from '@/components/ui/table';
import type { FluOpportunity } from '@/lib/campaign-types';
import { CheckIcon, SendIcon, EyeIcon } from 'lucide-react';

interface OpportunityRowProps {
  opportunity: FluOpportunity;
  onView: (id: string) => void;
  onApprove: (id: string) => void;
  onSend: (id: string) => void;
}

const statusVariant: Record<string, 'default' | 'secondary' | 'outline'> = {
  pending: 'outline',
  approved: 'secondary',
  sent: 'default',
  converted: 'default',
  declined: 'outline',
};

export function OpportunityRow({
  opportunity,
  onView,
  onApprove,
  onSend,
}: OpportunityRowProps) {
  return (
    <TableRow>
      <TableCell className="font-medium">{opportunity.siblingMrn}</TableCell>
      <TableCell>{opportunity.siblingName}</TableCell>
      <TableCell>{opportunity.subjectName}</TableCell>
      <TableCell>{opportunity.appointmentDate}</TableCell>
      <TableCell>{opportunity.appointmentLocation}</TableCell>
      <TableCell>
        {opportunity.hasAsthma && (
          <Badge variant="destructive" className="text-xs">
            Asthma
          </Badge>
        )}
      </TableCell>
      <TableCell>
        <Badge variant={statusVariant[opportunity.status] ?? 'outline'}>
          {opportunity.status}
        </Badge>
      </TableCell>
      <TableCell>
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            onClick={() => onView(opportunity.id)}
          >
            <EyeIcon className="h-4 w-4" />
          </Button>
          {opportunity.status === 'pending' && (
            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8"
              onClick={() => onApprove(opportunity.id)}
            >
              <CheckIcon className="h-4 w-4" />
            </Button>
          )}
          {opportunity.status === 'approved' && (
            <Button
              variant="ghost"
              size="icon"
              className="h-8 w-8"
              onClick={() => onSend(opportunity.id)}
            >
              <SendIcon className="h-4 w-4" />
            </Button>
          )}
        </div>
      </TableCell>
    </TableRow>
  );
}
