import { Badge } from '@/components/ui/badge';
import type { OpportunityStatus } from '@/lib/campaign-types';
import { SearchIcon } from 'lucide-react';

interface CampaignFiltersProps {
  searchQuery: string;
  onSearchChange: (query: string) => void;
  activeStatus: OpportunityStatus | 'all';
  onStatusChange: (status: OpportunityStatus | 'all') => void;
  asthmaOnly: boolean;
  onAsthmaChange: (asthmaOnly: boolean) => void;
}

const statuses: { label: string; value: OpportunityStatus | 'all' }[] = [
  { label: 'All', value: 'all' },
  { label: 'Pending', value: 'pending' },
  { label: 'Approved', value: 'approved' },
  { label: 'Sent', value: 'sent' },
  { label: 'Converted', value: 'converted' },
];

export function CampaignFilters({
  searchQuery,
  onSearchChange,
  activeStatus,
  onStatusChange,
  asthmaOnly,
  onAsthmaChange,
}: CampaignFiltersProps) {
  return (
    <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
      <div className="relative">
        <SearchIcon className='-translate-y-1/2 absolute top-1/2 left-3 h-4 w-4 text-muted-foreground' />
        <input
          type="text"
          placeholder="Search by MRN or name..."
          value={searchQuery}
          onChange={(e) => onSearchChange(e.target.value)}
          className="h-9 w-full rounded-md border border-input bg-background px-9 py-1 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus-visible:outline-hidden focus-visible:ring-1 focus-visible:ring-ring sm:w-64"
        />
      </div>
      <div className="flex flex-wrap items-center gap-2">
        {statuses.map((s) => (
          <Badge
            key={s.value}
            variant={activeStatus === s.value ? 'default' : 'outline'}
            className="cursor-pointer"
            onClick={() => onStatusChange(s.value)}
          >
            {s.label}
          </Badge>
        ))}
        <Badge
          variant={asthmaOnly ? 'destructive' : 'outline'}
          className="cursor-pointer"
          onClick={() => onAsthmaChange(!asthmaOnly)}
        >
          Asthma
        </Badge>
      </div>
    </div>
  );
}
