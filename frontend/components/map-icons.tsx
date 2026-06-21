import type { SVGProps } from "react";

// Lightweight inline renditions of the Google Maps and Apple Maps marks, used on
// the "Directions" links. They evoke the familiar brand icons — a red Google
// Maps pin and a blue Apple Maps tile with a navigation arrow — without pulling
// in external logo assets. Sized via CSS (.directions-link__icon).

export function GoogleMapsIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 24 24" width="1em" height="1em" aria-hidden="true" focusable="false" {...props}>
      <path
        d="M12 2C7.86 2 4.5 5.36 4.5 9.5c0 5.06 6.06 11.36 7.05 12.36.25.25.65.25.9 0C13.44 20.86 19.5 14.56 19.5 9.5 19.5 5.36 16.14 2 12 2z"
        fill="#EA4335"
      />
      <circle cx="12" cy="9.5" r="2.75" fill="#ffffff" />
    </svg>
  );
}

export function AppleMapsIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 24 24" width="1em" height="1em" aria-hidden="true" focusable="false" {...props}>
      <rect x="2" y="2" width="20" height="20" rx="5.5" fill="#3C8CFF" />
      <path
        d="M16.9 7.1 8.3 11.05c-.74.34-.7 1.42.06 1.71l3.02 1.13 1.13 3.02c.29.76 1.37.8 1.71.06l3.95-8.6c.3-.66-.38-1.34-1.04-1.04z"
        fill="#ffffff"
      />
    </svg>
  );
}
