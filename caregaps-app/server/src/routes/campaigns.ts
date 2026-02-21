import {
  Router,
  type Request,
  type Response,
  type Router as RouterType,
} from 'express';
import { authMiddleware, requireAuth } from '../middleware/auth';
import { executeSql } from '../lib/databricks-sql';

const CAMPAIGN_TABLE = 'dev_kiddo.silver.campaign_opportunities';

// Maps URL route types to DB campaign_type values
const DB_CAMPAIGN_TYPE: Record<string, string> = {
  'flu-vaccine': 'FLU_VACCINE',
};

const campaignNames: Record<string, string> = {
  'flu-vaccine': 'Flu Vaccine Piggybacking',
  'depression-screening': 'Depression Screening (PHQ-9)',
  'lab-piggybacking': 'Lab Piggybacking',
};

// ---------------------------------------------------------------------------
// Message templates — generates piggybacking messages from opportunity data.
// TODO: Move to dev_kiddo.silver.campaign_templates so operations can manage
// templates without code changes.
// ---------------------------------------------------------------------------

function getFirstName(fullName: string): string {
  if (!fullName) return '';
  // Handle "LASTNAME, FIRSTNAME" format from Epic
  if (fullName.includes(',')) {
    return fullName.split(',')[1]?.trim().split(' ')[0] ?? fullName;
  }
  return fullName.split(' ')[0] ?? fullName;
}

function formatDate(dateStr: string): string {
  try {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return dateStr;
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  } catch {
    return dateStr;
  }
}

function generatePiggybackMessage(opp: {
  siblingName: string;
  subjectName: string;
  appointmentDate: string;
  appointmentLocation: string;
  hasAsthma: boolean;
  lastFluVaccineDate: string | null;
}): string {
  const sibling = getFirstName(opp.siblingName);
  const subject = getFirstName(opp.subjectName);
  const date = formatDate(opp.appointmentDate);
  const loc = opp.appointmentLocation;
  const isSibling =
    opp.siblingName.trim().toLowerCase() !==
    opp.subjectName.trim().toLowerCase();

  let msg: string;

  if (isSibling) {
    if (opp.hasAsthma) {
      msg =
        `Hi! ${sibling} is due for a flu shot & has asthma, making flu protection extra important. ` +
        `Bring them to ${subject}'s appt at ${loc} on ${date}.`;
    } else {
      msg =
        `Hi! ${sibling} is due for a flu shot. ` +
        `Bring them to ${subject}'s appt at ${loc} on ${date} — easy to get it done in one trip!`;
    }
  } else {
    if (opp.hasAsthma) {
      msg =
        `Hi ${sibling}! You're due for a flu shot. With asthma, flu protection is extra important. ` +
        `Get it at your ${date} appt at ${loc}.`;
    } else {
      msg =
        `Hi ${sibling}! You're due for a flu shot. ` +
        `Get it at your ${date} appt at ${loc} — quick and easy while you're there!`;
    }
  }

  return msg.slice(0, 160);
}

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

    const fluStats = statsMap['FLU_VACCINE'] ?? {
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
      const conditions: string[] = [`campaign_type = 'FLU_VACCINE'`];
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
        WHERE campaign_type = 'FLU_VACCINE'
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

      const opportunities = opportunityRows.map((row, idx) => {
        const opp = {
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
          llmMessage: '',
          lastFluVaccineDate: row.last_flu_vaccine_date
            ? String(row.last_flu_vaccine_date)
            : null,
        };
        // Generate piggybacking message from template
        opp.llmMessage = generatePiggybackMessage(opp);
        return opp;
      });

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
 * POST /api/campaigns/:type/approve - Approve an opportunity
 * Body: { siblingMrn, subjectMrn }
 */
campaignsRouter.post(
  '/:type/approve',
  requireAuth,
  async (req: Request, res: Response) => {
    const type = String(req.params.type);
    const dbType = DB_CAMPAIGN_TYPE[type];
    if (!dbType) {
      res.status(400).json({ error: 'Unknown campaign type' });
      return;
    }

    const { siblingMrn, subjectMrn } = req.body;
    if (!siblingMrn || !subjectMrn) {
      res
        .status(400)
        .json({ error: 'Missing siblingMrn or subjectMrn' });
      return;
    }

    try {
      const sMrn = String(siblingMrn).replace(/'/g, "''");
      const xMrn = String(subjectMrn).replace(/'/g, "''");
      await executeSql(`
        UPDATE ${CAMPAIGN_TABLE}
        SET status = 'approved'
        WHERE campaign_type = '${dbType}'
          AND patient_mrn = '${sMrn}'
          AND subject_mrn = '${xMrn}'
          AND status = 'pending'
      `);
      res.json({ success: true, status: 'approved' });
    } catch (error) {
      console.error('[campaigns] Failed to approve:', error);
      res.status(500).json({
        error: 'Failed to approve opportunity',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  },
);

/**
 * POST /api/campaigns/:type/send - Mark opportunity as sent
 * Body: { siblingMrn, subjectMrn }
 */
campaignsRouter.post(
  '/:type/send',
  requireAuth,
  async (req: Request, res: Response) => {
    const type = String(req.params.type);
    const dbType = DB_CAMPAIGN_TYPE[type];
    if (!dbType) {
      res.status(400).json({ error: 'Unknown campaign type' });
      return;
    }

    const { siblingMrn, subjectMrn } = req.body;
    if (!siblingMrn || !subjectMrn) {
      res
        .status(400)
        .json({ error: 'Missing siblingMrn or subjectMrn' });
      return;
    }

    try {
      const sMrn = String(siblingMrn).replace(/'/g, "''");
      const xMrn = String(subjectMrn).replace(/'/g, "''");
      await executeSql(`
        UPDATE ${CAMPAIGN_TABLE}
        SET status = 'sent'
        WHERE campaign_type = '${dbType}'
          AND patient_mrn = '${sMrn}'
          AND subject_mrn = '${xMrn}'
          AND status = 'approved'
      `);
      res.json({ success: true, status: 'sent' });
    } catch (error) {
      console.error('[campaigns] Failed to send:', error);
      res.status(500).json({
        error: 'Failed to send message',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  },
);
