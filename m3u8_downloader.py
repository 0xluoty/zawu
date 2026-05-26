
import asyncio
import aiohttp
import m3u8
import os
import shutil
import subprocess
import sys
from urllib.parse import urljoin, urlparse
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from tqdm import tqdm
from tqdm.asyncio import tqdm as asynctqdm

class M3U8Downloader:
    def __init__(self, m3u8_url, output_path, concurrency=16, max_retries=3):
        self.m3u8_url = m3u8_url
        
        target_path = os.path.abspath(output_path)
        
        if os.path.isdir(target_path) or output_path.endswith(('/', '\\')):
            self.output_dir = target_path
            self.output_filename = "video.mp4"
            self.output_full_path = os.path.join(self.output_dir, self.output_filename)
        else:
            self.output_dir = os.path.dirname(target_path)
            self.output_filename = os.path.basename(target_path)
            self.output_full_path = target_path
            
            if not os.path.splitext(self.output_filename)[1]:
                self.output_filename += ".mp4"
                self.output_full_path += ".mp4"

        self.concurrency = concurrency
        self.max_retries = max_retries
        self.base_url = self._get_base_url(m3u8_url)
        self.segments = []
        self.keys = {}
        
        self.temp_dir = os.path.join(self.output_dir, f".temp_{int(asyncio.get_event_loop().time())}")
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir, exist_ok=True)

    def _get_base_url(self, url):
        parsed_url = urlparse(url)
        path = os.path.dirname(parsed_url.path)
        if not path.endswith('/'):
            path += '/'
        return f"{parsed_url.scheme}://{parsed_url.netloc}{path}"

    async def _fetch_content(self, session, url):
        async with session.get(url, timeout=30) as response:
            response.raise_for_status()
            return await response.read()

    async def _parse_m3u8(self, session):
        content = await self._fetch_content(session, self.m3u8_url)
        playlist = m3u8.loads(content.decode('utf-8', errors='ignore'), uri=self.m3u8_url)
        
        if playlist.is_variant:
            best_playlist = max(playlist.playlists, key=lambda p: p.stream_info.bandwidth if p.stream_info.bandwidth else 0)
            variant_url = urljoin(self.m3u8_url, best_playlist.uri)
            print(f"检测到主播放列表，选择最高码率变体: {variant_url}")
            content = await self._fetch_content(session, variant_url)
            playlist = m3u8.loads(content.decode('utf-8', errors='ignore'), uri=variant_url)

        for i, segment in enumerate(playlist.segments):
            segment_url = urljoin(playlist.base_uri or self.base_url, segment.uri)
            key_info = None
            if segment.key:
                key_url = urljoin(playlist.base_uri or self.base_url, segment.key.uri)
                iv = segment.key.iv
                key_info = {'method': segment.key.method, 'uri': key_url, 'iv': iv, 'index': i}
            self.segments.append({'url': segment_url, 'key_info': key_info, 'index': i})
        
        print(f"解析完成，共有 {len(self.segments)} 个分段。")

    async def _get_key(self, session, key_url):
        if key_url not in self.keys:
            self.keys[key_url] = await self._fetch_content(session, key_url)
        return self.keys[key_url]

    async def _download_segment(self, session, segment_data):
        url = segment_data['url']
        index = segment_data['index']
        key_info = segment_data['key_info']
        file_path = os.path.join(self.temp_dir, f"{index:05d}.ts")
        
        if os.path.exists(file_path):
            return True

        for attempt in range(self.max_retries):
            try:
                content = await self._fetch_content(session, url)
                
                if key_info:
                    key = await self._get_key(session, key_info['uri'])
                    if key_info['iv']:
                        iv = bytes.fromhex(key_info['iv'][2:]) if key_info['iv'].startswith('0x') else key_info['iv'].encode()
                    else:
                        iv = index.to_bytes(16, byteorder='big')
                    
                    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
                    decryptor = cipher.decryptor()
                    content = decryptor.update(content) + decryptor.finalize()

                with open(file_path, 'wb') as f:
                    f.write(content)
                return True
            except Exception as e:
                if attempt == self.max_retries - 1:
                    return False
                await asyncio.sleep(1)
        return False

    async def download(self):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=self.concurrency)) as session:
            try:
                await self._parse_m3u8(session)
            except Exception as e:
                print(f"解析 M3U8 失败: {e}")
                return
            
            if not self.segments:
                print("未发现有效分段。")
                return

            queue = asyncio.Queue()
            for seg in self.segments:
                await queue.put(seg)

            async def worker(pbar):
                while not queue.empty():
                    segment_data = await queue.get()
                    await self._download_segment(session, segment_data)
                    pbar.update(1)
                    queue.task_done()

            with asynctqdm(total=len(self.segments), desc="下载进度") as pbar:
                workers = [asyncio.create_task(worker(pbar)) for _ in range(self.concurrency)]
                await queue.join()
                for w in workers:
                    w.cancel()

        self._merge_segments()

    def _merge_segments(self):
        files = sorted([f for f in os.listdir(self.temp_dir) if f.endswith(".ts")])
        if not files:
            print("没有下载到任何分段，合并取消。")
            return

        print(f"开始合并 {len(files)} 个分段...")
        
        temp_merged = os.path.join(self.output_dir, f"temp_merged_{int(asyncio.get_event_loop().time())}.ts")
        
        try:
            with open(temp_merged, 'wb') as outfile:
                with tqdm(total=len(files), desc="合并进度") as pbar:
                    for file in files:
                        file_path = os.path.join(self.temp_dir, file)
                        with open(file_path, 'rb') as infile:
                            outfile.write(infile.read())
                        pbar.update(1)

            output_ext = os.path.splitext(self.output_full_path)[1].lower()
            if output_ext in ['.mp4', '.mkv', '.mov']:
                print(f"正在转换格式并修正时间戳...")
                try:
                    cmd = ["ffmpeg", "-i", temp_merged, "-c", "copy", "-y", self.output_full_path]
                    subprocess.run(cmd, check=True, capture_output=True)
                    if os.path.exists(temp_merged):
                        os.remove(temp_merged)
                    print(f"\n[成功] 视频已保存至: {self.output_full_path}")
                except Exception as e:
                    print(f"FFmpeg 处理失败，直接保存原始合并文件: {e}")
                    final_ts = os.path.splitext(self.output_full_path)[0] + ".ts"
                    if os.path.exists(final_ts): os.remove(final_ts)
                    os.rename(temp_merged, final_ts)
                    print(f"已保存为: {final_ts}")
            else:
                if os.path.exists(self.output_full_path):
                    os.remove(self.output_full_path)
                os.rename(temp_merged, self.output_full_path)
                print(f"\n[成功] 视频已保存至: {self.output_full_path}")
        except Exception as e:
            print(f"合并过程中出错: {e}")
        finally:
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir, ignore_errors=True)

async def main():
    print("===  M3U8 下载器  ===")
    url = input("M3U8 链接: ").strip()
    if not url:
        print("错误: 链接不能为空")
        return
    
    save_path = input("保存路径 (文件名或文件夹): ").strip()
    if not save_path:
        save_path = "video.mp4"
    
    threads_input = input("并发数 (默认 16): ").strip()
    threads = int(threads_input) if threads_input.isdigit() else 16
    
    downloader = M3U8Downloader(url, save_path, concurrency=threads)
    await downloader.download()

if __name__ == "__main__":
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n用户取消下载。")
    except Exception as e:
        print(f"\n程序意外终止: {e}")
