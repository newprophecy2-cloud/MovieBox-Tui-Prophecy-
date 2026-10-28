# Playback delivery modes

Playback descriptors preserve three distinct delivery paths:

| Source/path | Delivery behavior |
| --- | --- |
| Verified, compatible MP4-family media with upstream range support | `/api/v1/sources/{source_id}/direct/{token}` passes media through and preserves validated byte-range responses. |
| Native HLS or DASH | `/api/v1/sources/{source_id}/native/{token}` relays manifests/resources and retains native segment and range behavior. These sources are not sent through FFmpeg remux. |
| Other verified non-native media that needs remuxing | `/api/v1/sources/{source_id}/remux` and its `/transcode` alias, including the catalog `/api/v1/catalog/sources/{source_id}/remux` route, progressively remux through FFmpeg pipes. |

The progressive route opens the validated upstream response, feeds its body to FFmpeg stdin incrementally, and reads fragmented MP4 from FFmpeg stdout. FFmpeg uses `pipe:0` and `pipe:1` and fragmented-MP4 initialization/media-fragment flags; it does not receive an output filename. The HTTP response begins only after output includes a media fragment (`moof`), then writes are relayed in bounded chunks. Application-level startup and transfer buffers have fixed limits, writes await the client socket (backpressure), and the route does not create a full source or output video file or an all-output vector.

The progressive remux response has no `Content-Length` and does not advertise byte-range support. The server speaks HTTP/1.1 and uses connection-close framing rather than adding a conflicting transfer-encoding header. A `Range` request to the progressive remux endpoint receives `501 SEEK_UNAVAILABLE`; this is not arbitrary seeking. The route retains the configured media-job concurrency limit and job deadline; the upstream pump also has an idle timeout. Cancellation drops the upstream request and terminates the FFmpeg child when supported. If streaming fails after response headers are sent, the connection ends instead of receiving a second HTTP status.

This path is intended for sources FFmpeg can demux progressively from a non-seekable input. It does not guarantee that every container/source can start before FFmpeg finishes probing, a particular startup time, or future arbitrary seeking. Input containers that require seeking or place essential metadata at the end may fail before a playable fragment is produced. Seeking redesign and broader codec/audio/manifest policy remain out of scope.

The generic `/api/stream-proxy` endpoint follows the same forward-only, no-persistent-media-cache behavior and rejects nonzero timestamp seeks because its validated input is pipe-based. Playback descriptors for sources requiring remux use the progressive source route above.
