module.exports = async function handler(req, res) {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.setHeader('Access-Control-Max-Age', '86400');
    res.status(204).end();
    return;
  }

  if (req.method !== 'GET') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.status(405).json({ error: 'Method not allowed' });
    return;
  }

  try {
    const { file } = req.query || {};
    // Expect HH.json (00..23.json)
    if (typeof file !== 'string' || !/^\d{2}\.json$/.test(file)) {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.status(400).json({ error: 'Invalid file. Expected HH.json' });
      return;
    }

    const upstream = `https://a.windbornesystems.com/treasure/${file}`;
    const upstreamRes = await fetch(upstream, { cache: 'no-store' });
    const buf = Buffer.from(await upstreamRes.arrayBuffer());

    // Pass through content-type if available
    const contentType = upstreamRes.headers.get('content-type') || 'application/json; charset=utf-8';
    res.setHeader('Content-Type', contentType);
    // CORS + modest caching to reduce load
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Cache-Control', 'public, max-age=15, s-maxage=60');
    res.status(upstreamRes.status).send(buf);
  } catch (err) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.status(502).json({ error: 'Upstream fetch failed' });
  }
}


