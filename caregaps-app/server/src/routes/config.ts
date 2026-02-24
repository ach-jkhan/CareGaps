// ================================================================
// IMPROVED config.ts - Hospital Branding Configuration
// ================================================================
//
// This endpoint returns configuration to the frontend for branding
//
// ================================================================

import { Router, type Request, type Response } from 'express';

export const configRouter = Router();

/**
 * GET /api/config - Return app configuration including hospital branding
 */
configRouter.get('/', (_req: Request, res: Response) => {
  const config = {
    // Hospital Branding
    hospital: {
      name: process.env.HOSPITAL_NAME || "Akron Children's Hospital",
      logoUrl: process.env.HOSPITAL_LOGO_URL || 'assets/ACH_Logo_main.png',
      primaryColor: process.env.PRIMARY_COLOR || '#0066CC',
      secondaryColor: process.env.SECONDARY_COLOR || '#00A651',
      accentColor: process.env.ACCENT_COLOR || '#FFB81C',
    },

    // Support Information
    support: {
      email:
        process.env.SUPPORT_EMAIL ||
        'enterprisedataandanalytics@akronchildrens.org',
      phone: process.env.SUPPORT_PHONE || '(330) 543-1000',
      hours: process.env.SUPPORT_HOURS || 'Monday-Friday, 8 AM - 5 PM EST',
      /*portalUrl: process.env.SUPPORT_PORTAL_URL || 'https://intranet.akronchildrens.org/it',*/
    },

    // Feature Flags
    features: {
      chatHistory:
        process.env.ENABLE_CHAT_HISTORY === 'true' ||
        !!(process.env.PGDATABASE || process.env.POSTGRES_URL),
      phiAccess: process.env.ENABLE_PHI_ACCESS === 'true',
      multiModel: process.env.ENABLE_MULTI_MODEL === 'true',
      analytics: process.env.ENABLE_ANALYTICS === 'true',
    },

    // App Metadata
    app: {
      name: process.env.APP_NAME || 'Care Gaps Assistant',
      version: process.env.APP_VERSION || '1.0.0',
      environment: process.env.NODE_ENV || 'development',
    },

    // Agent Configuration
    agent: {
      endpoint: process.env.DATABRICKS_SERVING_ENDPOINT || '',
      maxRetries: Number.parseInt(process.env.MAX_RETRIES || '3'),
      timeout: Number.parseInt(process.env.AGENT_TIMEOUT || '45000'),
    },
  };

  res.json(config);
});

/**
 * GET /api/config/branding - Return only branding info (for public pages)
 */
configRouter.get('/branding', (_req: Request, res: Response) => {
  res.json({
    hospitalName: process.env.HOSPITAL_NAME || "Akron Children's Hospital",
    logoUrl: process.env.HOSPITAL_LOGO_URL || 'assets/ACH_Logo_main.png',
    primaryColor: process.env.PRIMARY_COLOR || '#0066CC',
    secondaryColor: process.env.SECONDARY_COLOR || '#00A651',
    tagline: process.env.HOSPITAL_TAGLINE || 'Pediatric Care Excellence',
  });
});
