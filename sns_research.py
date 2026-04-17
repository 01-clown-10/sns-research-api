"""
SNS競合調査エンドポイント
Apify Python Client を使用
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
from apify_client import ApifyClient

router = APIRouter(prefix="/sns", tags=["SNS競合調査"])

APIFY_API_TOKEN = os.getenv("APIFY_API_TOKEN")

ACTORS = {
    "tiktok":    "clockworks~tiktok-profile-scraper",
    "instagram": "apify~instagram-scraper",
    "youtube":   "streamers~youtube-scraper",
    "x":         "apidojo~tweet-scraper",
}


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
    accounts: list[AccountData]


def parse_tiktok(items: list[dict]) -> list[AccountData]:
    results = []
    for item in items:
        author = item.get("authorMeta", {})
        handle = author.get("name", "")
        followers = author.get("fans")
        hearts = author.get("heart")
        videos = author.get("video")
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


def parse_instagram(items: list[dict]) -> list[AccountData]:
    results = []
    for item in items:
        handle = item.get("username", "")
        results.append(AccountData(
            handle=f"@{handle}",
            display_name=item.get("fullName"),
            followers=item.get("followersCount"),
            following=item.get("followsCount"),
            posts=item.get("postsCount"),
            bio=item.get("biography"),
            url=f"https://www.instagram.com/{handle}/",
            platform="instagram",
        ))
    return results


def parse_youtube(items: list[dict]) -> list[AccountData]:
    results = []
    for item in items:
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


def parse_x(items: list[dict]) -> list[AccountData]:
    results = []
    for item in items:
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


def build_actor_input(platform: str, handles: list[str], max_results: int) -> dict:
    clean = [h.lstrip("@") for h in handles]
    if platform == "tiktok":
        return {
            "profiles": clean,
            "resultsType": "details",
            "maxPostsPerProfile": 1,  # 課金を最小化：1アカウント1動画のみ
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
            "handles": clean,
            "tweetsDesired": 1,
        }
    return {}


@router.get("/research", response_model=ResearchResponse)
async def research_accounts_get(
    handles: str,
    platform: str,
    max_results: Optional[int] = 20,
):
    """
    GETエンドポイント（Claude web_fetch対応）
    例: GET /sns/research?handles=aaa,bbb&platform=tiktok&max_results=10
    """
    if not APIFY_API_TOKEN:
        raise HTTPException(status_code=500, detail="APIFY_API_TOKEN が未設定です")

    handle_list = [h.strip() for h in handles.split(",") if h.strip()]
    if not handle_list:
        raise HTTPException(status_code=400, detail="handles が空です")

    platform_lower = platform.lower()
    if platform_lower not in ACTORS:
        raise HTTPException(status_code=400, detail=f"未対応のプラットフォーム: {platform}")

    actor_input = build_actor_input(platform_lower, handle_list, max_results)

    try:
        client = ApifyClient(APIFY_API_TOKEN)
        run = client.actor(ACTORS[platform_lower]).call(run_input=actor_input)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Apify実行エラー: {str(e)}")

    accounts = PARSERS[platform_lower](items)

    # ハンドルで重複除去（最初に出現したものを残す）
    seen = set()
    unique_accounts = []
    for acc in accounts:
        if acc.handle not in seen:
            seen.add(acc.handle)
            unique_accounts.append(acc)

    unique_accounts.sort(key=lambda a: a.followers if a.followers is not None else -1, reverse=True)

    return ResearchResponse(platform=platform_lower, total=len(unique_accounts), accounts=unique_accounts)


@router.get("/health")
async def health():
    return {"status": "ok", "apify_token_set": bool(APIFY_API_TOKEN)}
