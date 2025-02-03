import { ConfigEnv, defineConfig, loadEnv  } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig((mode: ConfigEnv) => {
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  const env = loadEnv(mode.mode, process.cwd());
  const baseUrl = env.VITE_BASE_URL
  return {
    base: '/ui/preview',
    plugins: [react()],
    server: {
      proxy: {
        ['/ui/preview/auth']: {
          target: baseUrl,
          changeOrigin: true,
        },
        ['/ui/api']: {
          target: baseUrl,
          changeOrigin: true,
        },
      }
    }
  }
})
