import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { TableCell, TableRow } from '@/components/ui/table';
import type { FluOpportunity } from '@/lib/campaign-types';
import {
  CheckIcon,
  SendIcon,
  EyeIcon,
  EyeOffIcon,
  MessageSquareTextIcon,
} from 'lucide-react';

function formatDate(dateStr: string | null): string {
  if (!dateStr) return '';
  try {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return dateStr;
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    const yyyy = d.getFullYear();
    return `${mm}/${dd}/${yyyy}`;
  } catch {
    return dateStr;
  }
}

interface OpportunityRowProps {
  opportunity: FluOpportunity;
  isExpanded: boolean;
  onToggleExpand: () => void;
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
  isExpanded,
  onToggleExpand,
  onApprove,
  onSend,
}: OpportunityRowProps) {
  return (
    <>
      <TableRow
        className={isExpanded ? 'border-b-0 bg-muted/30' : undefined}
      >
        <TableCell className="font-medium">
          {opportunity.siblingMrn}
        </TableCell>
        <TableCell>{opportunity.siblingName}</TableCell>
        <TableCell>{opportunity.subjectName}</TableCell>
        <TableCell>{formatDate(opportunity.appointmentDate)}</TableCell>
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
              onClick={onToggleExpand}
            >
              {isExpanded ? (
                <EyeOffIcon className="h-4 w-4" />
              ) : (
                <EyeIcon className="h-4 w-4" />
              )}
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
      {isExpanded && (
        <TableRow className="bg-muted/30 hover:bg-muted/30">
          <TableCell colSpan={8} className="py-3">
            <div className="flex items-start gap-2 pl-1">
              <MessageSquareTextIcon className="mt-0.5 h-4 w-4 shrink-0 text-blue-500" />
              <div className="text-sm">
                <span className="font-medium text-muted-foreground">
                  Suggested message:{' '}
                </span>
                <span>{opportunity.llmMessage}</span>
                {opportunity.lastFluVaccineDate && (
                  <span className="ml-2 text-xs text-muted-foreground">
                    (Last vaccine: {formatDate(opportunity.lastFluVaccineDate)})
                  </span>
                )}
              </div>
            </div>
          </TableCell>
        </TableRow>
      )}
    </>
  );
}
