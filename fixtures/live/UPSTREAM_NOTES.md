# Live upstream evidence captured 2026-11-06

- Dramachi public site: https://dramachi.com/
- Dramachi API: https://api.nodeobjects.com/
- 4KHDHub public site: https://4khdhub.one/
- 4KHDHub anime category: https://4khdhub.one/category/anime/
- 4KHDHub current detail fixture source: https://4khdhub.one/the-iceblade-sorcerer-shall-rule-the-world-series-6914/

Observed Dramachi behavior:

- `interface=search` returns real anime records for Haibane Renmei (`42999`), Mawaru Penguindrum (`56148`), and Ergo Proxy (`41062`). Den-noh Coil and Texhnolyze returned `data:null`.
- `interface=title_v2` returns season metadata and `interface=eplist` returns episode records with `fid`, `disk`, `quality`, and filenames.
- `interface=getFile&fid=145126&findex=2` returned a valid MP4 path and host. The constructed URL `https://er2e2w.pdkvzojivp.xyz/cdn/zone1/Anime/Haibane_Renmei/Season_01/Haibane_Renmei_S01E13_KimoiTV.com.mp4` returned HTTP 200 with `video/mp4`; the same path without `/cdn/` returned HTTP 500.
- The upstream file response does not provide a codec label. It must be media-probed before playback.

Observed 4KHDHub behavior:

- Current search HTML still uses `a.movie-card`, `.movie-card-title`, and `.movie-card-meta`.
- Current anime detail HTML still uses `#episodes`, `.episode-download-item`, `.episode-file-title`, `.badge-size`, and real `greenmotors.club` HubCloud/HubDrive links.
- The current search endpoint returned unrelated or empty results for the five requested historical titles in this capture; no matching content was fabricated.
- A live Black Clover page was also captured from the existing ignored acceptance test. Its current 4KHDHub detail page is `https://4khdhub.one/black-clover-series-122/` and contains S01E01 through S01E51 only. The test's requested E67 is therefore upstream catalog drift, not a parser omission; the parser must not synthesize an episode that the provider does not publish.
