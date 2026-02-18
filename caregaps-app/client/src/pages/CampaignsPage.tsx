import { Link } from 'react-router-dom';
import useSWR from 'swr';
import { SidebarToggle } from '@/components/sidebar-toggle';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { fetcher } from '@/lib/utils';
import type { Campaign } from '@/lib/campaign-types';
import { SyringeIcon, BrainIcon, FlaskConicalIcon } from 'lucide-react';

const campaignDescriptions: Record<string, string> = {
  'flu-vaccine':
    'Identify siblings overdue for flu vaccine when a household member has an upcoming appointment.',
  'depression-screening':
    'Adolescent depression screening reminders for patients due for PHQ-9.',
  'lab-piggybacking':
    'Combine overdue lab orders with upcoming appointments for efficiency.',
};

const campaignIcons: Record<string, typeof SyringeIcon> = {
  'flu-vaccine': SyringeIcon,
  'depression-screening': BrainIcon,
  'lab-piggybacking': FlaskConicalIcon,
};

interface CampaignsResponse {
  campaigns: Campaign[];
}

export default function CampaignsPage() {
  const { data, isLoading } = useSWR<CampaignsResponse>(
    '/api/campaigns',
    fetcher,
  );

  const campaigns = data?.campaigns;

  return (
    <div className="flex flex-col">
      <header className="sticky top-0 flex items-center gap-2 bg-background px-2 py-1.5 md:px-2">
        <SidebarToggle />
        <h1 className="font-semibold text-lg">Campaigns</h1>
      </header>
      <div className="flex-1 space-y-6 p-4 md:p-6">
        <div>
          <p className="text-muted-foreground text-sm">
            Manage outreach campaigns for care gap closure.
          </p>
        </div>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {isLoading
            ? Array.from({ length: 3 }).map((_, i) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: static skeleton
                <Card key={i}>
                  <CardHeader>
                    <Skeleton className="h-5 w-24" />
                    <Skeleton className="mt-2 h-5 w-48" />
                    <Skeleton className="mt-1 h-4 w-64" />
                  </CardHeader>
                  <CardContent>
                    <Skeleton className="h-4 w-40" />
                  </CardContent>
                </Card>
              ))
            : campaigns?.map((campaign) => {
                const Icon = campaignIcons[campaign.type] ?? SyringeIcon;
                return (
                  <Link key={campaign.type} to={`/campaigns/${campaign.type}`}>
                    <Card className="transition-colors hover:bg-muted/50">
                      <CardHeader>
                        <div className="flex items-center justify-between">
                          <Icon className="h-5 w-5 text-muted-foreground" />
                          <Badge
                            variant={
                              campaign.status === 'active'
                                ? 'default'
                                : 'secondary'
                            }
                          >
                            {campaign.status}
                          </Badge>
                        </div>
                        <CardTitle className="text-lg">
                          {campaign.name}
                        </CardTitle>
                        <CardDescription>
                          {campaignDescriptions[campaign.type] ?? ''}
                        </CardDescription>
                      </CardHeader>
                      <CardContent>
                        <div className="flex gap-4 text-muted-foreground text-sm">
                          <span>{campaign.stats.total} total</span>
                          <span>{campaign.stats.pending} pending</span>
                          <span>{campaign.stats.sent} sent</span>
                          <span>{campaign.stats.converted} converted</span>
                        </div>
                      </CardContent>
                    </Card>
                  </Link>
                );
              })}
        </div>
      </div>
    </div>
  );
}
