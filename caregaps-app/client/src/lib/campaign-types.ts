export type CampaignType =
  | 'flu-vaccine'
  | 'depression-screening'
  | 'lab-piggybacking';

export type CampaignStatus = 'active' | 'paused' | 'completed' | 'draft';

export type OpportunityStatus =
  | 'pending'
  | 'approved'
  | 'sent'
  | 'converted'
  | 'declined';

export interface Campaign {
  type: CampaignType;
  name: string;
  description: string;
  status: CampaignStatus;
  stats: CampaignStats;
}

export interface CampaignStats {
  total: number;
  pending: number;
  sent: number;
  converted: number;
}

export interface FluOpportunity {
  id: string;
  subjectMrn: string;
  subjectName: string;
  siblingMrn: string;
  siblingName: string;
  appointmentDate: string;
  appointmentLocation: string;
  hasAsthma: boolean;
  lastFluVaccineDate: string | null;
  myChartActive: boolean;
  mobilePhone: string | null;
  status: OpportunityStatus;
  llmMessage: string | null;
}
