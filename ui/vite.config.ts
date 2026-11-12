/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In development the page is served by Vite and talks to a locally running marketdata
// process (`cmd/marketdata`, default port 8083) through these proxies, so the browser
// only ever sees one origin and no CORS headers are needed anywhere.
const marketdata = process.env.MARKETDATA ?? "http://localhost:8083";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/ws": { target: marketdata, ws: true },
      "/api": { target: marketdata },
    },
  },
  build: { sourcemap: true },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test-setup.ts"],
  },
});
