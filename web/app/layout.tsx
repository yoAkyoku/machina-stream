import type { Metadata, Viewport } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Machina Stream | 工廠監測",
  description: "合成工廠即時事件與告警監測。唯讀展示，不連接或控制實體機台。",
};

export const viewport: Viewport = {
  themeColor: "#1d2a30",
  colorScheme: "dark",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="zh-Hant">
      <body>{children}</body>
    </html>
  );
}
