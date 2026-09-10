import httpx
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
import random

class DcContentCrawler:
    def __init__(self, gallery_id: str):
        self.gallery_id = gallery_id
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Referer": f"https://gall.dcinside.com/mgallery/board/lists?id={gallery_id}"
        }

    async def fetch_page_list(self, page: int):
        """1단계: 게시글 목록 가져오기"""
        url = "https://gall.dcinside.com/mgallery/board/lists"
        params = {"id": self.gallery_id, "page": page}
        
        async with httpx.AsyncClient(headers=self.headers, follow_redirects=True) as client:
            try:
                response = await client.get(url, params=params, timeout=10.0)
                return self.parse_list(response.text)
            except Exception as e:
                print(f"❌ 목록 로드 에러 ({page}p): {e}")
                return []

    def parse_list(self, html: str):
        soup = BeautifulSoup(html, 'html.parser')
        rows = soup.select('tr.ub-content')
        articles = []
        for row in rows:
            num_tag = row.select_one('.gall_num')
            if num_tag and num_tag.text.strip().isdigit():
                title_tag = row.select_one('.gall_tit a')
                link = "https://gall.dcinside.com" + title_tag['href']
                articles.append({"no": num_tag.text.strip(), "title": title_tag.text.strip(), "link": link})
        return articles

    async def fetch_content(self, session, article):
        """2단계: 게시글 상세 페이지에서 본문 추출"""
        # 디시 차단 방지를 위한 미세 대기
        await asyncio.sleep(random.uniform(0.3, 0.8))
        
        try:
            response = await session.get(article['link'], timeout=10.0)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                # 디시 본문 영역 셀렉터: .write_div
                content_tag = soup.select_one('.write_div')
                content = content_tag.get_text(separator="\n", strip=True) if content_tag else ""
                
                # 데이터 합치기
                article['content'] = content
                print(f"✅ 수집 완료: [{article['no']}] {article['title'][:15]}...")
                return article
        except Exception as e:
            print(f"❌ 본문 수집 실패 ({article['no']}): {e}")
            article['content'] = ""
            return article

async def main():
    gallery_id = "krstock"
    crawler = DcContentCrawler(gallery_id)
    
    # 1. 먼저 1페이지 목록 수집
    print(f"🚀 [{gallery_id}] 1페이지 목록 수집 중...")
    article_list = await crawler.fetch_page_list(1)
    
    if not article_list:
        print("목록을 가져오지 못했습니다.")
        return

    # 2. 수집된 목록의 링크를 타고 들어가서 본문 수집 (병렬 처리)
    print(f"📝 총 {len(article_list)}개 게시글 본문 수집 시작...")
    
    async with httpx.AsyncClient(headers=crawler.headers, follow_redirects=True) as session:
        tasks = [crawler.fetch_content(session, article) for article in article_list]
        final_results = await asyncio.gather(*tasks)

    # 3. 결과 확인
    df = pd.DataFrame(final_results)
    print("\n" + "="*50)
    print(f"📊 최종 수집 결과 (총 {len(df)}건)")
    print("="*50)
    # 내용(content)이 있는 데이터만 상위 5개 출력
    print(df[df['content'] != ""][['no', 'title', 'content']].head())
    
    # CSV 저장 (LLM 분석용 데이터셋)
    df.to_csv("dc_content_data.csv", index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    asyncio.run(main())