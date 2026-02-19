import { useState } from 'react';
import { useParams } from 'react-router-dom';
import useSWR from 'swr';
import { toast } from 'sonner';
import { SidebarToggle } from '@/components/sidebar-toggle';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Skeleton } from '@/components/ui/skeleton';
import { CampaignStats } from '@/components/campaigns/campaign-stats';
import { CampaignFilters } from '@/components/campaigns/campaign-filters';
import { OpportunitiesTable } from '@/components/campaigns/opportunities-table';
import { fetcher } from '@/lib/utils';
import type {
  CampaignStats as CampaignStatsType,
  FluOpportunity,
  OpportunityStatus,
} from '@/lib/campaign-types';

const campaignNames: Record<string, string> = {
  'flu-vaccine': 'Flu Vaccine Piggybacking',
  'depression-screening': 'Depression Screening (PHQ-9)',
  'lab-piggybacking': 'Lab Piggybacking',
};

interface CampaignDetailResponse {
  type: string;
  name: string;
  stats: CampaignStatsType;
  opportunities: FluOpportunity[];
}

export default function CampaignDetailPage() {
  const { type } = useParams<{ type: string }>();
  const [searchQuery, setSearchQuery] = useState('');
  const [activeStatus, setActiveStatus] = useState<OpportunityStatus | 'all'>(
    'all',
  );
  const [asthmaOnly, setAsthmaOnly] = useState(false);

  const campaignName = campaignNames[type ?? ''] ?? 'Campaign';

  // Build query string for server-side filtering
  const params = new URLSearchParams();
  if (activeStatus !== 'all') params.set('status', activeStatus);
  if (searchQuery) params.set('search', searchQuery);
  if (asthmaOnly) params.set('asthmaOnly', 'true');
  const qs = params.toString();
  const url = `/api/campaigns/${type}${qs ? `?${qs}` : ''}`;

  const { data, isLoading, mutate } = useSWR<CampaignDetailResponse>(
    url,
    fetcher,
    { keepPreviousData: true },
  );

  const stats: CampaignStatsType = data?.stats ?? {
    total: 0,
    pending: 0,
    sent: 0,
    converted: 0,
  };
  const opportunities: FluOpportunity[] = data?.opportunities ?? [];

  const handleView = (id: string) => {
    const opp = opportunities.find((o) => o.id === id);
    if (!opp) return;
    if (opp.llmMessage) {
      toast.info(opp.llmMessage, { duration: 8000 });
    } else {
      toast.info(
        `${opp.siblingName} (MRN: ${opp.siblingMrn}) — scheduled with ${opp.subjectName} on ${opp.appointmentDate} at ${opp.appointmentLocation}${opp.hasAsthma ? ' | Asthma: Yes' : ''}${opp.lastFluVaccineDate ? ` | Last flu vaccine: ${opp.lastFluVaccineDate}` : ''}`,
        { duration: 8000 },
      );
    }
  };

  const handleApprove = async (id: string) => {
    const opp = opportunities.find((o) => o.id === id);
    if (!opp) return;
    try {
      const res = await fetch(`/api/campaigns/${type}/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          siblingMrn: opp.siblingMrn,
          subjectMrn: opp.subjectMrn,
        }),
      });
      if (!res.ok) throw new Error('Failed to approve');
      toast.success(`Approved: ${opp.siblingName}`);
      mutate();
    } catch {
      toast.error('Failed to approve opportunity');
    }
  };

  const handleSend = async (id: string) => {
    const opp = opportunities.find((o) => o.id === id);
    if (!opp) return;
    try {
      const res = await fetch(`/api/campaigns/${type}/send`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          siblingMrn: opp.siblingMrn,
          subjectMrn: opp.subjectMrn,
        }),
      });
      if (!res.ok) throw new Error('Failed to send');
      toast.success(`Message sent for ${opp.siblingName}`);
      mutate();
    } catch {
      toast.error('Failed to send message');
    }
  };

  return (
    <div className="flex flex-col">
      <header className="sticky top-0 flex items-center gap-2 bg-background px-2 py-1.5 md:px-2">
        <SidebarToggle />
        <h1 className="font-semibold text-lg">{campaignName}</h1>
      </header>
      <div className="flex-1 space-y-6 p-4 md:p-6">
        <Tabs defaultValue="opportunities">
          <TabsList>
            <TabsTrigger value="opportunities">Opportunities</TabsTrigger>
            <TabsTrigger value="metrics">Metrics</TabsTrigger>
          </TabsList>
          <TabsContent value="opportunities" className="space-y-6">
            {isLoading && !data ? (
              <div className="space-y-4">
                <div className="grid gap-4 md:grid-cols-4">
                  {Array.from({ length: 4 }).map((_, i) => (
                    // biome-ignore lint/suspicious/noArrayIndexKey: static skeleton
                    <Skeleton key={i} className="h-20 rounded-lg" />
                  ))}
                </div>
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-64 w-full" />
              </div>
            ) : (
              <>
                <CampaignStats stats={stats} />
                <CampaignFilters
                  searchQuery={searchQuery}
                  onSearchChange={setSearchQuery}
                  activeStatus={activeStatus}
                  onStatusChange={setActiveStatus}
                  asthmaOnly={asthmaOnly}
                  onAsthmaChange={setAsthmaOnly}
                />
                <OpportunitiesTable
                  opportunities={opportunities}
                  onView={handleView}
                  onApprove={handleApprove}
                  onSend={handleSend}
                />
              </>
            )}
          </TabsContent>
          <TabsContent value="metrics">
            <div className="flex h-64 items-center justify-center rounded-md border text-muted-foreground">
              Campaign metrics coming soon
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
