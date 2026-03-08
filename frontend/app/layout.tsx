import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Art Activity Collection",
  description: "Search free kids and teen art activities",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
