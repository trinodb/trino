import { ConfigEnv, defineConfig, loadEnv, Plugin } from 'vite'
import react from '@vitejs/plugin-react'

// Fontsource CSS declares a legacy .woff fallback after each .woff2 source; dropping it
// keeps the deprecated .woff files out of the bundle (woff2 is supported since ~2016)
const stripWoffFallback = (): Plugin => ({
    name: 'strip-woff-fallback',
    enforce: 'pre',
    transform(code: string, id: string) {
        if (id.includes('@fontsource') && id.endsWith('.css')) {
            return code.replace(/,\s*url\([^)]*\.woff\)\s*format\('woff'\)/g, '')
        }
    },
})

// https://vitejs.dev/config/
export default defineConfig((mode: ConfigEnv) => {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    const env = loadEnv(mode.mode, process.cwd())
    const baseUrl = env.VITE_BASE_URL
    return {
        base: '/ui',
        plugins: [stripWoffFallback(), react()],
        server: {
            proxy: {
                ['/ui/auth']: {
                    target: baseUrl,
                    changeOrigin: true,
                },
                ['/ui/api']: {
                    target: baseUrl,
                    changeOrigin: true,
                },
            },
        },
    }
})
