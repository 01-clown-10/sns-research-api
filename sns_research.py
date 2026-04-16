"""
SNS競合調査エンドポイント
SynapScale FastAPI に追加するモジュール

使い方:
  main.py に以下を追加:
    from sns_research import router as sns_router
    app.include_router(sns_router)
"""

import asyncio
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import os

router = APIRouter(prefix="/sns", tags=["SNS競合調査"])

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")

# ---- Apify Actor ID ----
ACTORS = {
    "tiktok":    "clockworks~tiktok-profile-scraper",
    "instagram": "apify~instagram-scraper",
    "youtube":   "streamers~youtube-scraper",
    "x":         "apidojo~tweet-scraper",
}


# ---- リクエスト/レスポンス型 ----

class ResearchRequest(BaseModel):
    handles: list[str]          # 例: ["@hoge", "fuga"]
    platform: str               # "tiktok" | "instagram" | "youtube" | "x"
    max_results: Optional[int] = 20


class AccountData(BaseModel):
    handle: str
    display_name: Optional[str] = None
    followers: Optional[int] = None
    following: Optional[int] = None
    posts: Optional[int] = None
    engagement_rate: Optional[float] = None
    bio: Optional[str] = None
    url: Optional[str] = None
    platform: str


class ResearchResponse(BaseModel):
    platform: str
    total: int
    accounts: list[AccountData]  # フォロワー降順ソート済み


# ---- Apify 共通呼び出し ----

async def run_apify_actor(actor_id: str, input_data: dict) -> list[dict]:
    """
    Apify Actor を同期的に実行してデータを返す
    タイムアウト: 120秒
    """
    if not APIFY_API_TOKEN:
        raise HTTPException(status_code=500, detail="APIFY_API_TOKEN が未設定です")

    base_url = "https://api.apify.com/v2"
    headers = {"Authorization": f"Bearer {APIFY_API_TOKEN}"}

    async with httpx.AsyncClient(timeout=120) as client:
        # Actor 実行開始
        run_resp = await client.post(
            f"{base_url}/acts/{actor_id}/runs",
            headers=headers,
            json={"input": input_data},
            params={"waitForFinish": 120},   # 最大120秒同期待機
        )
        if run_resp.status_code not in (200, 201):
            raise HTTPException(
                status_code=502,
                detail=f"Apify Actor 起動失敗: {run_resp.text}"
            )

        run_data = run_resp.json().get("data", {})
        dataset_id = run_data.get("defaultDatasetId")

        if not dataset_id:
            raise HTTPException(status_code=502, detail="DatasetID が取得できませんでした")

        # データセット取得
        dataset_resp = await client.get(
            f"{base_url}/datasets/{dataset_id}/items",
            headers=headers,
            params={"format": "json"},
        )
        return dataset_resp.json() if dataset_resp.status_code == 200 else []


# ---- プラットフォーム別パーサー ----

def parse_tiktok(raw: list[dict], handles: list[str]) -> list[AccountData]:
    results = []
    for item in raw:
        author = item.get("authorMeta", {})
        handle = author.get("name", "")
        followers = author.get("fans", None)
        hearts = author.get("heart", None)
        videos = author.get("video", None)

        # ER概算 = 総いいね ÷ フォロワー ÷ 動画数（動画あたり平均ER）
        er = None
        if followers and hearts and videos and followers > 0 and videos > 0:
            er = round((hearts / videos / followers) * 100, 2)

        results.append(AccountData(
            handle=f"@{handle}",
            display_name=author.get("nickName"),
            followers=followers,
            following=author.get("following"),
            posts=videos,
            engagement_rate=er,
            bio=author.get("signature"),
            url=f"https://www.tiktok.com/@{handle}",
            platform="tiktok",
        ))
    return results


def parse_instagram(raw: list[dict], handles: list[str]) -> list[AccountData]:
    results = []
    for item in raw:
        handle = item.get("username", "")
        followers = item.get("followersCount", None)
        posts = item.get("postsCount", None)
        results.append(AccountData(
            handle=f"@{handle}",
            display_name=item.get("fullName"),
            followers=followers,
            following=item.get("followsCount"),
            posts=posts,
            bio=item.get("biography"),
            url=f"https://www.instagram.com/{handle}/",
            platform="instagram",
        ))
    return results


def parse_youtube(raw: list[dict], handles: list[str]) -> list[AccountData]:
    results = []
    for item in raw:
        handle = item.get("channelId", "")
        results.append(AccountData(
            handle=handle,
            display_name=item.get("channelName"),
            followers=item.get("numberOfSubscribers"),
            posts=item.get("numberOfVideos"),
            bio=item.get("channelDescription"),
            url=item.get("channelUrl"),
            platform="youtube",
        ))
    return results


def parse_x(raw: list[dict], handles: list[str]) -> list[AccountData]:
    results = []
    for item in raw:
        handle = item.get("userName", "")
        results.append(AccountData(
            handle=f"@{handle}",
            display_name=item.get("name"),
            followers=item.get("followers"),
            following=item.get("following"),
            posts=item.get("statusesCount"),
            bio=item.get("description"),
            url=f"https://x.com/{handle}",
            platform="x",
        ))
    return results


PARSERS = {
    "tiktok":    parse_tiktok,
    "instagram": parse_instagram,
    "youtube":   parse_youtube,
    "x":         parse_x,
}


# ---- Actor 入力ビルダー ----

def build_actor_input(platform: str, handles: list[str], max_results: int) -> dict:
    # @を除去して正規化
    clean = [h.lstrip("@") for h in handles]

    if platform == "tiktok":
        return {
            "profiles": clean,
            "resultsType": "details",
            "maxProfilesPerQuery": max_results,
        }
    elif platform == "instagram":
        return {
            "usernames": clean,
            "resultsType": "details",
            "resultsLimit": max_results,
        }
    elif platform == "youtube":
        return {
            "startUrls": [{"url": f"https://www.youtube.com/@{h}"} for h in clean],
            "maxResults": max_results,
        }
    elif platform == "x":
        return {
            "handle": clean,
            "tweetsDesired": 1,   # プロフィール情報のみ取得
        }
    return {}


# ---- エンドポイント ----

@router.post("/research", response_model=ResearchResponse)
async def research_accounts(req: ResearchRequest):
    """
    ハンドルリストを受け取り、Apify で各アカウントのフォロワー数等を取得して
    フォロワー降順のリストを返す
    """
    platform = req.platform.lower()
    if platform not in ACTORS:
        raise HTTPException(
            status_code=400,
            detail=f"未対応のプラットフォーム: {platform}。対応: {list(ACTORS.keys())}"
        )

    actor_id = ACTORS[platform]
    actor_input = build_actor_input(platform, req.handles, req.max_results)

    # Apify 実行
    raw = await run_apify_actor(actor_id, actor_input)

    # パース
    parser = PARSERS[platform]
    accounts = parser(raw, req.handles)

    # フォロワー降順ソート（不明は末尾）
    accounts.sort(
        key=lambda a: a.followers if a.followers is not None else -1,
        reverse=True,
    )

    return ResearchResponse(
        platform=platform,
        total=len(accounts),
        accounts=accounts,
    )


@router.get("/health")
async def health():
    return {"status": "ok", "apify_token_set": bool(APIFY_API_TOKEN)}


@router.get("/research", response_model=ResearchResponse)
async def research_accounts_get(
    handles: str,
    platform: str,
    max_results: Optional[int] = 20,
):
    """
    GETエンドポイント版（Claude web_fetch対応）
    使い方: GET /sns/research?handles=aaa,bbb,ccc&platform=tiktok&max_results=10
    """
    handle_list = [h.strip() for h in handles.split(",") if h.strip()]
    if not handle_list:
        raise HTTPException(status_code=400, detail="handles が空です")

    platform_lower = platform.lower()
    if platform_lower not in ACTORS:
        raise HTTPException(
            status_code=400,
            detail=f"未対応のプラットフォーム: {platform}。対応: {list(ACTORS.keys())}"
        )

    actor_id = ACTORS[platform_lower]
    actor_input = build_actor_input(platform_lower, handle_list, max_results)

    raw = await run_apify_actor(actor_id, actor_input)

    parser = PARSERS[platform_lower]
    accounts = parser(raw, handle_list)

    accounts.sort(
        key=lambda a: a.followers if a.followers is not None else -1,
        reverse=True,
    )

    return ResearchResponse(
        platform=platform_lower,
        total=len(accounts),
        accounts=accounts,
    )
