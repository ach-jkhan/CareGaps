import { useState } from 'react';
import { useParams } from 'react-router-dom';
import { toast } from 'sonner';
import { SidebarToggle } from '@/components/sidebar-toggle';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { CampaignStats } from '@/components/campaigns/campaign-stats';
import { CampaignFilters } from '@/components/campaigns/campaign-filters';
import { OpportunitiesTable } from '@/components/campaigns/opportunities-table';
import type {
  CampaignStats as CampaignStatsType,
  FluOpportunity,
  OpportunityStatus,
} from '@/lib/campaign-types';

const mockStats: CampaignStatsType = {
  total: 1247,
  pending: 892,
  sent: 234,
  converted: 121,
};

const mockOpportunities: FluOpportunity[] = [
  {
    id: '1',
    subjectMrn: 'MRN-100234',
    subjectName: 'Tommy Johnson',
    siblingMrn: 'MRN-100235',
    siblingName: 'Sarah Johnson',
    appointmentDate: '2026-02-18',
    appointmentLocation: 'Akron - Considine',
    hasAsthma: true,
    lastFluVaccineDate: '2025-01-15',
    myChartActive: true,
    mobilePhone: '330-555-0142',
    status: 'pending',
    llmMessage:
      "Great job getting Sarah's flu shot last Jan! Since flu vaccines protect for one year, she's due again. Sarah's asthma makes this extra important. Bring her to Tommy's appt at Considine on 2/18!",
  },
  {
    id: '2',
    subjectMrn: 'MRN-200456',
    subjectName: 'Emma Rodriguez',
    siblingMrn: 'MRN-200457',
    siblingName: 'Jake Rodriguez',
    appointmentDate: '2026-02-20',
    appointmentLocation: 'Beachwood',
    hasAsthma: false,
    lastFluVaccineDate: '2024-11-20',
    myChartActive: true,
    mobilePhone: '216-555-0198',
    status: 'approved',
    llmMessage:
      "Jake did great getting his flu vaccine last Nov! Each vaccine protects for one year, so he's due. Someone in your household has an appt at Beachwood on 2/20 - bring Jake for his flu shot!",
  },
];

const campaignNames: Record<string, string> = {
  'flu-vaccine': 'Flu Vaccine Piggybacking',
  'depression-screening': 'Depression Screening (PHQ-9)',
  'lab-piggybacking': 'Lab Piggybacking',
};

export default function CampaignDetailPage() {
  const { type } = useParams<{ type: string }>();
  const [searchQuery, setSearchQuery] = useState('');
  const [activeStatus, setActiveStatus] = useState<OpportunityStatus | 'all'>(
    'all',
  );
  const [asthmaOnly, setAsthmaOnly] = useState(false);

  const campaignName = campaignNames[type ?? ''] ?? 'Campaign';

  const filteredOpportunities = mockOpportunities.filter((opp) => {
    if (
      searchQuery &&
      !opp.siblingMrn.toLowerCase().includes(searchQuery.toLowerCase()) &&
      !opp.siblingName.toLowerCase().includes(searchQuery.toLowerCase()) &&
      !opp.subjectName.toLowerCase().includes(searchQuery.toLowerCase())
    ) {
      return false;
    }
    if (activeStatus !== 'all' && opp.status !== activeStatus) {
      return false;
    }
    if (asthmaOnly && !opp.hasAsthma) {
      return false;
    }
    return true;
  });

  const handleView = (id: string) => {
    const opp = mockOpportunities.find((o) => o.id === id);
    if (opp?.llmMessage) {
      toast.info(opp.llmMessage, { duration: 5000 });
    }
  };

  const handleApprove = (id: string) => {
    toast.success(`Opportunity ${id} approved`);
  };

  const handleSend = (id: string) => {
    toast.success(`Message sent for opportunity ${id}`);
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
            <CampaignStats stats={mockStats} />
            <CampaignFilters
              searchQuery={searchQuery}
              onSearchChange={setSearchQuery}
              activeStatus={activeStatus}
              onStatusChange={setActiveStatus}
              asthmaOnly={asthmaOnly}
              onAsthmaChange={setAsthmaOnly}
            />
            <OpportunitiesTable
              opportunities={filteredOpportunities}
              onView={handleView}
              onApprove={handleApprove}
              onSend={handleSend}
            />
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
