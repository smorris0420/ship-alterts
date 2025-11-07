<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:output method="html" indent="yes"/>
  <xsl:template match="/">
    <html>
      <head>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title><xsl:value-of select="rss/channel/title"/></title>
        <style>
          :root{
            --dcl-navy:#16578A;     /* page background + brand color */
            --dcl-gold:#C9A227;     /* trim accent */
            --ink:#1b1b1b;          /* body text on white */
            --muted:#6b6f76;
            --bg:#16578A;           /* page background (blue) */
            --card:#ffffff;         /* card background (white) */
            --line:#e9edf2;
            --pill:#eef4fb;
          }
          *{box-sizing:border-box}
          body{
            margin:0;
            background:var(--bg);       /* BLUE page behind cards */
            color:var(--ink);
            font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,Helvetica,sans-serif;
          }

          /* White header bar */
          .bar{
            background:#ffffff;
            color:var(--dcl-navy);
            padding:14px 18px;
            border-bottom:4px solid var(--dcl-gold);
          }

          /* Center logo + title (stacked) */
          .brand{
            display:flex;
            flex-direction:column;
            align-items:center;       /* centered */
            text-align:center;        /* centered text */
            gap:6px;
            max-width:1100px;
            margin:0 auto;
          }
          .logo-img{
            width:325px;height:auto;display:block;
            margin:0 auto;            /* center the image */
          }
          .brand h1{
            margin:0;
            font-size:18px;
            line-height:1.2;
            font-weight:700;
            color:var(--dcl-navy);    /* title in blue */
          }

          .wrap{max-width:1100px;margin:18px auto;padding:0 16px}
          .card{
            background:var(--card);
            border-radius:10px;
            box-shadow:0 6px 18px rgba(0,0,0,.10);
            border:1px solid var(--line);
          }
          .meta{
            padding:14px 16px;
            display:flex;flex-wrap:wrap;gap:12px;align-items:center;
            border-bottom:1px solid var(--line);
            color:var(--muted);font-size:12px;
          }
          .meta a{color:var(--dcl-navy);text-decoration:underline}
          .chip{
            background:var(--pill);color:var(--dcl-navy);
            border:1px solid #d7e5f6;padding:4px 8px;border-radius:999px;
            font-size:12px;font-weight:600;
          }

          table{width:100%;border-collapse:collapse;font-size:14px;background:#fff}
          thead th{
            position:sticky;top:0;background:#fbfdff;z-index:1;
            text-align:left;padding:12px 14px;border-bottom:2px solid var(--line);
            color:#133c5e;font-weight:700;
          }
          tbody td{padding:12px 14px;border-bottom:1px solid var(--line);vertical-align:top;}
          tbody tr:hover{background:#fbfdff}
          .title a{color:var(--dcl-navy);text-decoration:none;font-weight:700}
          .title a:hover{text-decoration:underline}
          .guid{font-family:ui-monospace,Menlo,Consolas,monospace;color:var(--muted);font-size:12px}
          .desc{white-space:pre-wrap}

          .badge{
            display:inline-block;padding:3px 8px;border-radius:6px;
            font-weight:700;font-size:12px;border:1px solid transparent;margin-right:8px;
          }
          .arr{background:#e8f6ee;color:#11643a;border-color:#cfead9}
          .dep{background:#fff0f0;color:#8a1620;border-color:#ffd9de}

          @media (max-width:760px){
            thead{display:none}
            tbody tr{display:block;border-bottom:8px solid #f0f4f8}
            tbody td{display:block;border:0;padding:8px 14px}
            tbody td::before{content:attr(data-label) " ";font-weight:600;color:var(--muted);display:block;margin-bottom:2px}
            .brand{gap:8px}
          }
        </style>
      </head>
      <body>
        <div class="bar">
          <div class="brand">
            <img src="DCLDailySummary.png" alt="DCL Logo" class="logo-img"/>
            <h1><xsl:value-of select="rss/channel/title"/></h1>
          </div>
        </div>

        <div class="wrap">
          <div class="card">
            <div class="meta">
              <span class="chip">DCL • Airport &amp; Resort Reporting</span>
              <span><strong>Feed link:</strong> <a href="{rss/channel/link}"><xsl:value-of select="rss/channel/link"/></a></span>
              <span><strong>Last Build:</strong> <xsl:value-of select="rss/channel/lastBuildDate"/></span>
            </div>

            <table role="table" aria-label="Items">
              <thead>
                <tr><th>Title</th><th>Published</th><th>Description</th></tr>
              </thead>
              <tbody>
                <xsl:for-each select="rss/channel/item">
                  <tr>
                    <td class="title" data-label="Title">
                      <span class="badge">
                        <xsl:attribute name="class">
                          <xsl:text>badge </xsl:text>
                          <xsl:choose>
                            <xsl:when test="contains(title,'Arrived')">arr</xsl:when>
                            <xsl:otherwise>dep</xsl:otherwise>
                          </xsl:choose>
                        </xsl:attribute>
                        <xsl:choose>
                          <xsl:when test="contains(title,'Arrived')">ARRIVED</xsl:when>
                          <xsl:otherwise>DEPARTED</xsl:otherwise>
                        </xsl:choose>
                      </span>
                      <a href="{link}"><xsl:value-of select="title"/></a><br/>
                      <span class="guid"><xsl:value-of select="guid"/></span>
                    </td>
                    <td data-label="Published"><xsl:value-of select="pubDate"/></td>
                    <td class="desc" data-label="Description">
                      <xsl:value-of select="description" disable-output-escaping="yes"/>
                    </td>
                  </tr>
                </xsl:for-each>
              </tbody>
            </table>
          </div>
        </div>
      </body>
    </html>
  </xsl:template>
</xsl:stylesheet>
