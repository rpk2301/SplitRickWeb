# SplitRickWeb

## Ricasso game invites

- `/play/<code>` redirects to `public/play.html?code=<code>` (rule in `redirects.json`, applied to the Amplify app).
- `public/apple-app-site-association` lists `/play/*` for `98Z45822HV.com.RichardKyte.Scrawl`, so the link opens the Ricasso app straight into that lobby when it's installed.
- The iMessage preview image is `public/og-ricasso.png`, drawn by `og-ricasso.swift` (run `swift og-ricasso.swift` from the repo root to re-render it).
