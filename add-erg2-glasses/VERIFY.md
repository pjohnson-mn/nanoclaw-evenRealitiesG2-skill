# Verify G2 Glasses Channel

### Health check

```bash
curl http://localhost:7420/health
```

Should return `{"status":"ok","channel":"erg2-glasses"}`.

### Send a test message

```bash
curl -X POST http://localhost:7420/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <your-token>" \
  -d '{"model":"nanoclaw","messages":[{"role":"user","content":"Hello from the glasses"}]}'
```

Should return an OpenAI chat completion response with the agent's reply within ~20 seconds. Check `logs/nanoclaw.log` for the `ERG2 glasses request` entry confirming routing.
