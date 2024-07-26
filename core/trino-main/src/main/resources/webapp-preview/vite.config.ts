import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  base: '/ui/preview',
  plugins: [react()],
  server: {
    proxy: {
      '/ui/api': 'http://localhost:8080',
    }
  }
})
