import {
  Router,
  type Request,
  type Response,
  type Router as RouterType,
} from 'express';
import { authMiddleware, requireAuth } from '../middleware/auth';
import { executeSql } from '../lib/databricks-sql';

const CAMPAIGN_TABLE = 'dev_kiddo.silver.campaign_opportunities';

const campaignNames: Record<string, string> = {
  'flu-vaccine': 'Flu Vaccine Piggybacking',
  'depression-screening': 'Depression Screening (PHQ-9)',
  'lab-piggybacking': 'Lab Piggybacking',
};

export const campaignsRouter: RouterType = Router();

campaignsRouter.use(authMiddleware);

/**
 * GET /api/campaigns - List all campaigns with real stats
 */
campaignsRouter.get('/', requireAuth, async (_req: Request, res: Response) => {
  try {
    const rows = await executeSql(`
      SELECT
        campaign_type,
        COUNT(*) as total,
        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as converted
      FROM ${CAMPAIGN_TABLE}
      GROUP BY campaign_type
    `);

    // Build a lookup of stats by campaign_type
    const statsMap: Record<
      string,
      { total: number; pending: number; sent: number; converted: number }
    > = {};
    for (const row of rows) {
      statsMap[String(row.campaign_type)] = {
        total: Number(row.total) || 0,
        pending: Number(row.pending) || 0,
        sent: Number(row.sent) || 0,
        converted: Number(row.converted) || 0,
      };
    }

    const fluStats = statsMap['flu-vaccine'] ?? {
      total: 0,
      pending: 0,
      sent: 0,
      converted: 0,
    };

    res.json({
      campaigns: [
        {
          type: 'flu-vaccine',
          name: 'Flu Vaccine Piggybacking',
          status: fluStats.total > 0 ? 'active' : 'draft',
          stats: fluStats,
        },
        {
          type: 'depression-screening',
          name: 'Depression Screening (PHQ-9)',
          status: 'draft',
          stats: { total: 0, pending: 0, sent: 0, converted: 0 },
        },
        {
          type: 'lab-piggybacking',
          name: 'Lab Piggybacking',
          status: 'draft',
          stats: { total: 0, pending: 0, sent: 0, converted: 0 },
        },
      ],
    });
  } catch (error) {
    console.error('[campaigns] Failed to fetch campaign stats:', error);
    res.status(500).json({
      error: 'Failed to fetch campaign data',
      message: error instanceof Error ? error.message : 'Unknown error',
    });
  }
});

/**
 * GET /api/campaigns/:type - Get campaign detail with opportunities
 * Query params: ?status=pending&search=smith&asthmaOnly=true&limit=100
 */
campaignsRouter.get(
  '/:type',
  requireAuth,
  async (req: Request, res: Response) => {
    const type = String(req.params.type);
    const name = campaignNames[type] ?? 'Campaign';

    // Draft campaigns have no data yet
    if (type !== 'flu-vaccine') {
      res.json({
        type,
        name,
        stats: { total: 0, pending: 0, sent: 0, converted: 0 },
        opportunities: [],
      });
      return;
    }

    try {
      const statusFilter = req.query.status as string | undefined;
      const search = req.query.search as string | undefined;
      const asthmaOnly = req.query.asthmaOnly === 'true';
      const limit = Math.min(Number(req.query.limit) || 100, 500);

      // Build WHERE clauses — campaign_type is always included
      const conditions: string[] = [`campaign_type = 'flu-vaccine'`];
      if (statusFilter && statusFilter !== 'all') {
        conditions.push(`status = '${statusFilter.replace(/'/g, "''")}'`);
      }
      if (search) {
        const escaped = search.replace(/'/g, "''");
        conditions.push(
          `(LOWER(patient_name) LIKE LOWER('%${escaped}%') ` +
            `OR LOWER(subject_name) LIKE LOWER('%${escaped}%') ` +
            `OR LOWER(patient_mrn) LIKE LOWER('%${escaped}%'))`,
        );
      }
      if (asthmaOnly) {
        conditions.push(`has_asthma = 'Y'`);
      }

      const whereClause = `WHERE ${conditions.join(' AND ')}`;

      // Fetch stats (unfiltered for the campaign header)
      const statsRows = await executeSql(`
        SELECT
          COUNT(*) as total,
          SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
          SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent,
          SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as converted
        FROM ${CAMPAIGN_TABLE}
        WHERE campaign_type = 'flu-vaccine'
      `);

      const statsRow = statsRows[0] ?? {};
      const stats = {
        total: Number(statsRow.total) || 0,
        pending: Number(statsRow.pending) || 0,
        sent: Number(statsRow.sent) || 0,
        converted: Number(statsRow.converted) || 0,
      };

      // Fetch opportunities with filters
      const opportunityRows = await executeSql(`
        SELECT
          patient_mrn,
          patient_name,
          subject_mrn,
          subject_name,
          appointment_date,
          appointment_location,
          has_asthma,
          mychart_active,
          mobile_number_on_file,
          status,
          llm_message,
          last_flu_vaccine_date
        FROM ${CAMPAIGN_TABLE}
        ${whereClause}
        ORDER BY appointment_date ASC
        LIMIT ${limit}
      `);

      const opportunities = opportunityRows.map((row, idx) => ({
        id: String(idx + 1),
        siblingMrn: row.patient_mrn ?? '',
        siblingName: row.patient_name ?? '',
        subjectMrn: row.subject_mrn ?? '',
        subjectName: row.subject_name ?? '',
        appointmentDate: row.appointment_date
          ? String(row.appointment_date)
          : '',
        appointmentLocation: row.appointment_location ?? '',
        hasAsthma: row.has_asthma === 'Y',
        myChartActive: row.mychart_active === 'Y',
        mobilePhone: row.mobile_number_on_file ?? null,
        status: row.status ?? 'pending',
        llmMessage: row.llm_message ?? null,
        lastFluVaccineDate: row.last_flu_vaccine_date
          ? String(row.last_flu_vaccine_date)
          : null,
      }));

      res.json({ type, name, stats, opportunities });
    } catch (error) {
      console.error('[campaigns] Failed to fetch opportunities:', error);
      res.status(500).json({
        error: 'Failed to fetch campaign data',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  },
);

/**
 * POST /api/campaigns/:type/approve - Approve an opportunity (stub)
 */
campaignsRouter.post(
  '/:type/approve',
  requireAuth,
  async (req: Request, res: Response) => {
    const { opportunityId } = req.body;
    res.json({ success: true, opportunityId, status: 'approved' });
  },
);

/**
 * POST /api/campaigns/:type/send - Send a message (stub)
 */
campaignsRouter.post(
  '/:type/send',
  requireAuth,
  async (req: Request, res: Response) => {
    const { opportunityId } = req.body;
    res.json({ success: true, opportunityId, status: 'sent' });
  },
);
