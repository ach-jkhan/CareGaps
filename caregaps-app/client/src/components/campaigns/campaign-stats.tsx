import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import type { CampaignStats as CampaignStatsType } from '@/lib/campaign-types';
import { UsersIcon, ClockIcon, SendIcon, CheckCircleIcon } from 'lucide-react';

interface CampaignStatsProps {
  stats: CampaignStatsType;
}

export function CampaignStats({ stats }: CampaignStatsProps) {
  const cards = [
    {
      title: 'Total Opportunities',
      value: stats.total,
      icon: UsersIcon,
      color: 'text-blue-600',
    },
    {
      title: 'Pending Review',
      value: stats.pending,
      icon: ClockIcon,
      color: 'text-yellow-600',
    },
    {
      title: 'Messages Sent',
      value: stats.sent,
      icon: SendIcon,
      color: 'text-green-600',
    },
    {
      title: 'Converted',
      value: stats.converted,
      icon: CheckCircleIcon,
      color: 'text-purple-600',
    },
  ];

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
      {cards.map((card) => (
        <Card key={card.title}>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className='font-medium text-sm'>
              {card.title}
            </CardTitle>
            <card.icon className={`h-4 w-4 ${card.color}`} />
          </CardHeader>
          <CardContent>
            <div className='font-bold text-2xl'>{card.value}</div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
