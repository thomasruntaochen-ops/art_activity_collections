type IconProps = { className?: string };

// Official brand marks served from /public/icons:
//  - google-maps.svg: the colorful Google Maps pin (Wikimedia "Google Maps icon (2020)").
//  - apple-maps.svg: the Apple Maps artwork (Wikimedia "Apple Maps (WatchOS)"),
//    re-cropped from a circle to a rounded square so it reads as the app icon.
// Decorative here (aria-hidden); the surrounding link carries the accessible name.

export function GoogleMapsIcon({ className }: IconProps) {
  return (
    <img src="/icons/google-maps.svg" alt="" aria-hidden="true" width={18} height={18} className={className} />
  );
}

export function AppleMapsIcon({ className }: IconProps) {
  return (
    <img src="/icons/apple-maps.svg" alt="" aria-hidden="true" width={18} height={18} className={className} />
  );
}
