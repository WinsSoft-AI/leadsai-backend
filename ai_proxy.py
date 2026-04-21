import base64
import logging
import os
from typing import Any, Dict, List, Optional

import httpx
from fastapi.encoders import jsonable_encoder

logger = logging.getLogger(__name__)


class AIProxy:
    """
    Proxies heavy AI requests (RAG, Gemini, CV, STT, TTS) to the dedicated AI_Backend.
    """

    def __init__(self):
        self.base_url = os.getenv("AI_BACKEND_URL", "http://localhost:8001")
        internal_token = os.getenv("AI_INTERNAL_TOKEN", "")
        # Use a longer timeout for LLM and heavy image processing
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=120.0,
            headers={"X-Internal-Token": internal_token},
        )
        logger.info(f"✅ AI Proxy connected to {self.base_url}")

    async def Check_Health(self):
        r = await self.client.get("/health")
        r.raise_for_status()
        return r.json()

    async def rag_retrieve(self, query: str, tenant_id: str, top_k: int = 5) -> List[str]:
        req = {"query": query, "tenant_id": tenant_id, "top_k": top_k}
        r = await self.client.post("/internal/rag/retrieve", json=req)
        r.raise_for_status()
        return r.json().get("chunks", [])

    async def rag_ingest(
        self, content: bytes, filename: str, content_type: str, tenant_id: str
    ) -> str:
        files = {"file": (filename, content, content_type)}
        data = {"tenant_id": tenant_id}
        r = await self.client.post("/internal/rag/ingest", data=data, files=files)
        r.raise_for_status()
        return r.json().get("job_id")

    async def rag_get_job_status(self, job_id: str) -> Dict[str, Any]:
        r = await self.client.get(f"/internal/rag/ingest/{job_id}")
        r.raise_for_status()
        return r.json()

    async def rag_delete_qa(self, qa_id: str, tenant_id: str) -> None:
        r = await self.client.delete(f"/internal/rag/qa/{qa_id}?tenant_id={tenant_id}")
        r.raise_for_status()

    async def rag_delete_doc(self, doc_id: str, tenant_id: str) -> None:
        r = await self.client.delete(f"/internal/rag/doc/{doc_id}?tenant_id={tenant_id}")
        r.raise_for_status()

    async def gemini_chat(
        self,
        message: str,
        history: List[Dict],
        context_chunks: List[str],
        tenant_config: Dict,
        language: str = "en",
    ) -> Dict[str, Any]:
        req = {
            "message": message,
            "history": history,
            "context_chunks": context_chunks,
            "tenant_config": tenant_config,
            "language": language,
        }
        
        # This converts datetimes to ISO strings and handles any Pydantic models
        
        try:
            serializable_req = jsonable_encoder(req)
            r = await self.client.post("/internal/gemini/chat", json=serializable_req)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 422:
                # This will print exactly which field failed validation
                logger.warning(f"Gemini validation error: {e.response.status_code}")
            raise e
    async def gemini_analyze_intent(self, history: List[Dict], tenant_config: Dict) -> Dict[str, Any]:
        req = {"history": history, "tenant_config": tenant_config}
        r = await self.client.post("/internal/gemini/analyze-intent", json=req)
        r.raise_for_status()
        return r.json()

    async def behavior_process(self, event_dict: Dict, tenant_id: str) -> Dict[str, Any]:
        req = {"event": event_dict, "tenant_id": tenant_id}
        r = await self.client.post("/internal/behavior/process", json=req)
        r.raise_for_status()
        return r.json()

    async def cv_search(self, image_bytes: bytes, tenant_id: str, top_k: int = 3) -> List[Dict]:
        files = {"file": ("image.jpg", image_bytes, "image/jpeg")}
        data = {"tenant_id": tenant_id, "top_k": str(top_k)}
        r = await self.client.post("/internal/cv/search", data=data, files=files)
        r.raise_for_status()
        return r.json().get("matches", [])

    async def stt_process(self, audio_chunk: bytes, session_id: str, language: str = "en") -> Dict[str, Any]:
        req = {
            "audio_b64": base64.b64encode(audio_chunk).decode(),
            "session_id": session_id,
            "language": language,
        }
        r = await self.client.post("/internal/stt/process", json=req)
        r.raise_for_status()
        return r.json()

    async def tts_synthesize(
        self, text: str, language: str = "en", session_id: Optional[str] = None
    ) -> Optional[str]:
        req = {"text": text, "language": language, "session_id": session_id}
        r = await self.client.post("/internal/tts/synthesize", json=req)
        r.raise_for_status()
        return r.json().get("audio_url")

    async def rag_ingest_qa(
        self, qa_id: str, question: str, answer: str, tenant_id: str
    ) -> Dict[str, Any]:
        req = {"qa_id": qa_id, "question": question, "answer": answer, "tenant_id": tenant_id}
        r = await self.client.post("/internal/rag/ingest-qa", json=req)
        r.raise_for_status()
        return r.json()

    async def rag_delete_qa_vector(self, qa_id: str, tenant_id: str) -> Dict[str, Any]:
        r = await self.client.request(
            "DELETE", f"/internal/rag/qa/{qa_id}", params={"tenant_id": tenant_id}
        )
        r.raise_for_status()
        return r.json()

    # ── Structured KB Proxy ────────────────────────────────────
    async def rag_rebuild_structured(
        self, tenant_id: str, chunks: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Flush structured KB chunks (company/product/QA) and re-ingest. Preserves file-upload chunks."""
        r = await self.client.post(
            "/internal/rag/rebuild",
            json={"tenant_id": tenant_id, "chunks": chunks},
            timeout=180.0,
        )
        r.raise_for_status()
        return r.json()

    async def scrape_single_page(self, url: str) -> Dict[str, Any]:
        """Fetch one URL and return extracted text for product enrichment."""
        r = await self.client.post(
            "/internal/scrape/single",
            json={"url": url},
            timeout=30.0,
        )
        r.raise_for_status()
        return r.json()

    async def enrich_product_from_text(self, text: str, url: str) -> Dict[str, Any]:
        """Use Gemini to extract structured product fields from raw page text."""
        r = await self.client.post(
            "/internal/gemini/enrich-product",
            json={"text": text, "url": url},
            timeout=60.0,
        )
        r.raise_for_status()
        return r.json()

    async def rag_get_stats(self, tenant_id: str) -> Dict[str, Any]:
        try:
            r = await self.client.get("/internal/rag/stats", params={"tenant_id": tenant_id}, timeout=5.0)
            r.raise_for_status()
            return {"status": "online", **r.json()}
        except Exception as e:
            logger.error(f"RAG stats failed: {e}")
            return {"status": "offline", "count": 0}

    async def rag_get_all_chunks(self, tenant_id: str) -> List[Dict[str, Any]]:
        try:
            r = await self.client.get("/internal/rag/chunks", params={"tenant_id": tenant_id}, timeout=15.0)
            r.raise_for_status()
            return r.json().get("chunks", [])
        except Exception as e:
            logger.error(f"RAG chunks fetch failed: {e}")
            return []

    async def system_info(self) -> Dict[str, Any]:
        try:
            # Short timeout here so the whole app doesn't hang if AI is down
            r = await self.client.get("/v1/system", timeout=5.0)
            r.raise_for_status()
            # 1. Parse the JSON body into a dictionary
            data = r.json()
        
        # 2. Check the dictionary, not the Response object
            if data.get("status") == "ok":
                logger.info(f"✅ AI Backend Online: {data}")
            else:
                logger.warning("⚠️ AI Backend returned unexpected status.")
                
            return data
        except Exception as e:
            logger.error(f"❌ AI Backend Error: {e}")
            return {"status": "offline", "service": "AI_Backend"}

    async def close(self):
        await self.client.aclose()
