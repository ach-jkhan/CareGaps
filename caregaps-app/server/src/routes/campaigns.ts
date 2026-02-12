import {
  Router,
  type Request,
  type Response,
  type Router as RouterType,
} from 'express';
import { authMiddleware, requireAuth } from '../middleware/auth';

export const campaignsRouter: RouterType = Router();

campaignsRouter.use(authMiddleware);

/**
 * GET /api/campaigns - List all campaigns
 */
campaignsRouter.get('/', requireAuth, async (_req: Request, res: Response) => {
  res.json({
    campaigns: [
      {
        type: 'flu-vaccine',
        name: 'Flu Vaccine Piggybacking',
        status: 'active',
        stats: { total: 1247, pending: 892, sent: 234, converted: 121 },
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
});

/**
 * GET /api/campaigns/:type - Get campaign detail with opportunities
 */
campaignsRouter.get(
  '/:type',
  requireAuth,
  async (req: Request, res: Response) => {
    const { type } = req.params;
    res.json({
      type,
      name:
        type === 'flu-vaccine'
          ? 'Flu Vaccine Piggybacking'
          : type === 'depression-screening'
            ? 'Depression Screening (PHQ-9)'
            : 'Lab Piggybacking',
      stats: { total: 1247, pending: 892, sent: 234, converted: 121 },
      opportunities: [],
    });
  },
);

/**
 * POST /api/campaigns/:type/approve - Approve an opportunity
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
 * POST /api/campaigns/:type/send - Send a message for an opportunity
 */
campaignsRouter.post(
  '/:type/send',
  requireAuth,
  async (req: Request, res: Response) => {
    const { opportunityId } = req.body;
    res.json({ success: true, opportunityId, status: 'sent' });
  },
);
