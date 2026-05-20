# Remove G2 Glasses Channel

1. Comment out `import './erg2-glasses.js';` in `src/channels/index.ts`
2. Remove `ERG2_GLASSES_TOKEN` and `ERG2_GLASSES_PORT` from `.env`
3. Rebuild: `pnpm run build`
4. Restart: `systemctl --user restart nanoclaw`
