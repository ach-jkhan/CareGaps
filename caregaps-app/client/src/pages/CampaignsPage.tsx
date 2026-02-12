import { Link } from 'react-router-dom';
import { SidebarToggle } from '@/components/sidebar-toggle';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import type { Campaign } from '@/lib/campaign-types';
import { SyringeIcon, BrainIcon, FlaskConicalIcon } from 'lucide-react';

const mockCampaigns: Campaign[] = [
  {
    type: 'flu-vaccine',
    name: 'Flu Vaccine Piggybacking',
    description:
      'Identify siblings overdue for flu vaccine when a household member has an upcoming appointment.',
    status: 'active',
    stats: { total: 1247, pending: 892, sent: 234, converted: 121 },
  },
  {
    type: 'depression-screening',
    name: 'Depression Screening (PHQ-9)',
    description:
      'Adolescent depression screening reminders for patients due for PHQ-9.',
    status: 'draft',
    stats: { total: 0, pending: 0, sent: 0, converted: 0 },
  },
  {
    type: 'lab-piggybacking',
    name: 'Lab Piggybacking',
    description:
      'Combine overdue lab orders with upcoming appointments for efficiency.',
    status: 'draft',
    stats: { total: 0, pending: 0, sent: 0, converted: 0 },
  },
];

const campaignIcons: Record<string, typeof SyringeIcon> = {
  'flu-vaccine': SyringeIcon,
  'depression-screening': BrainIcon,
  'lab-piggybacking': FlaskConicalIcon,
};

export default function CampaignsPage() {
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
          {mockCampaigns.map((campaign) => {
            const Icon = campaignIcons[campaign.type] ?? SyringeIcon;
            return (
              <Link key={campaign.type} to={`/campaigns/${campaign.type}`}>
                <Card className="transition-colors hover:bg-muted/50">
                  <CardHeader>
                    <div className="flex items-center justify-between">
                      <Icon className="h-5 w-5 text-muted-foreground" />
                      <Badge
                        variant={
                          campaign.status === 'active' ? 'default' : 'secondary'
                        }
                      >
                        {campaign.status}
                      </Badge>
                    </div>
                    <CardTitle className="text-lg">{campaign.name}</CardTitle>
                    <CardDescription>{campaign.description}</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className='flex gap-4 text-muted-foreground text-sm'>
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
