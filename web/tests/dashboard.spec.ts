import { expect, test } from "@playwright/test";

const api = process.env.MACHINA_API_URL ?? "http://127.0.0.1:18000";

function uuidV4(): string {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (character) => {
    const value = Math.floor(Math.random() * 16);
    return (character === "x" ? value : (value & 0x3) | 0x8).toString(16);
  });
}

test("live dashboard shows a persisted machine and its read-only alert", async ({ page, request }, testInfo) => {
  const machineId = `machine-ui-${uuidV4().slice(0, 8)}`;
  const response = await request.post(`${api}/api/v1/telemetry`, {
    data: {
      event_id: uuidV4(),
      schema_version: 1,
      factory_id: "factory-ui",
      line_id: "line-ui",
      machine_id: machineId,
      sequence_no: 1,
      event_time: new Date().toISOString(),
      metrics: { temperature_c: 98, vibration_rms_mm_s: 2, current_a: 20 },
      machine_status: "running",
      error_code: null,
    },
  });
  expect(response.status()).toBe(202);

  await page.goto("/");
  await expect(page.getByRole("heading", { name: "工廠總覽" })).toBeVisible();
  await expect(page.getByRole("link", { name: /API 文件/ })).toHaveAttribute(
    "href",
    `${new URL(api).origin}/docs`,
  );
  await expect(page.getByText(machineId, { exact: true }).first()).toBeVisible({ timeout: 15_000 });
  await expect(page.getByText("溫度偏高").first()).toBeVisible({ timeout: 15_000 });
  await expect(page.getByText("嚴重", { exact: true }).first()).toBeVisible();
  await expect(
    page.locator("#events").getByText(machineId, { exact: true }).first(),
  ).toBeVisible({ timeout: 15_000 });
  await expect(page.getByRole("button", { name: /確認|解除/ })).toHaveCount(0);

  await page.screenshot({ path: testInfo.outputPath("dashboard-desktop.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("heading", { name: "工廠總覽" })).toBeVisible();
  const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
  expect(documentWidth).toBeLessThanOrEqual(390);
  await page.screenshot({ path: testInfo.outputPath("dashboard-mobile.png"), fullPage: true });
});
