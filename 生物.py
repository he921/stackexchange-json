import re
import time
import requests
from bs4 import BeautifulSoup
import os
import json
import concurrent.futures
from fake_useragent import UserAgent
from threading import Lock  # 新增：用于线程安全的锁

class spider():
    def __init__(self):
        self.ua = UserAgent()
        self.proxy = {
        "http": "127.0.0.1:7890",
        "https": "127.0.0.1:7890"
    }
        self.header = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'zh-CN,zh;q=0.9',
    'priority': 'u=0, i',
    'referer': 'https://chemistry.stackexchange.com',
    'sec-ch-ua': '"Chromium";v="136", "Microsoft Edge";v="136", "Not.A/Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0',
    'cookie': 'prov=c0c11423-e240-4f03-9c51-dd0b07824b04; OptanonAlertBoxClosed=2025-04-28T09:48:49.063Z; __utma=27693923.1823193399.1745834966.1745835020.1745835020.1; __utmz=27693923.1745835020.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _ga_S812YQPLT2=GS1.1.1745828222.9.1.1745836180.0.0.0; _ga=GA1.1.1823193399.1745834966; chemistryuser=p=%5b160%7c%3bNewest%3b%3b%5d; __cf_bm=.wu.FIBEI6tZhp5S0Om95HIo9xrf2XyGYu6vpd__pjk-1747383607-1.0.1.1-YjlE4Xjoz3Z4VY1KaH50dJF12x1Mwx9w1A5BED.jfcEmUU8TSxzN6lbdQQ3iKCvcAYOX.JfccWwMDDygYnoiXUj8sRW3zWc3pLjfWiILjIg; _cfuvid=3Ju2yihWdXVznHlDEFM_MArOhe9GQKMYdNR8_g7SVtI-1747383607197-0.0.1.1-604800000; cf_clearance=XCw1lUMLYUryYKzjlb.RSUjoHYXiMHZrB3BHAJuZcaI-1747383630-1.2.1.1-rZUEgFy0075zUNkanrOt0a8VkVPVPpXDGHXzHiKrlF5HjdJqHHOL.CRC.4nJOk2_mkDUfJ1KtD39ttE0kw8YC03OlJZvzr.uI95DYsgdF5uFl7xvrHvcxllLKIlBkv3khAD.SdV7LuzvRYzKIOkyV7qmFpDE3oafCNetDG2sBMZ8lVtF9IPWdx76e0BgkffRQlkKDawXmzll91emGgcocSXrw1wNMq8XfmLAj8XqZzOeY0kWrJKMKk0qS4nHwspPtDtsDVoh7KCQH6gGWc8kk2LEdLTOKH8vXwpGOZ1APpX_x6_suNq2scHFNItiiNN6p_DdoF4YgbOQYdvk8cDQRnogHI8vGU8C.hKt.yZB3ic; OptanonConsent=isGpcEnabled=0&datestamp=Fri+May+16+2025+16%3A20%3A34+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202411.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=36d44472-5ba4-4dab-a30d-0804b17d2ee1&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false&intType=1&geolocation=HK%3B',
}

        self.json_file = "all_data_biology.json"  # JSON 文件名
        self.file_lock = Lock()  # 新增：文件写入锁
        self.physics_num = 0
        self.chemistry_num = 0
        self.math_num = 0
        self.biology_num = 0
        self.count=0
    def process_questions(self, questions):
        # 检查是否存在<img>标签
        if '<img' in questions:
            print("存在图片，开始下载图片...")
            # 提取所有<img>标签中的src属性
            image_links = re.findall(r'<img[^>]+src="([^"]+)"', questions)
            # 下载图片并获取本地路径
            local_paths = self.download_images(image_links)
            # 替换<img>标签中的src属性为本地路径，并删除alt属性
            for link, local_path in zip(image_links, local_paths):
                relative_path = os.path.relpath(local_path, start=os.path.dirname(__file__))
                # 替换src属性
                questions = questions.replace(f'src="{link}"', f'src="{relative_path}"')
                # 删除alt属性
                questions = re.sub(r' alt="[^"]*"', '', questions)
            print("图片下载完成，已替换为本地路径并删除alt属性。")
        else:
            print("没有图片，无需下载。")
        return questions

    def page_url(self, url, first_base_url):
        """处理分页URL，返回状态码"""
        try:
            response = requests.get(url, headers=self.header, proxies=self.proxy)
            response.raise_for_status()  # 触发HTTP错误时抛出异常
        except Exception as e:
            print(f"分页请求失败: {url} - {str(e)}")
            return "retry_page"  # 需要重试的标记

        soup = BeautifulSoup(response.text, 'html.parser')
        soup1 = soup.find("h2")

        # 判断是否为无效页面
        if soup1 and "No questions found" in soup1.text:
            self.count += 1
            if self.count >= 5:
                return 5  # 连续5次无效，终止当前站点
            return "invalid_page"  # 无效页面但未达终止条件
        else:
            self.count = 0  # 重置计数器

        # 正常处理逻辑
        s_links = soup.find_all(class_='s-link')
        solo_page_url = [first_base_url[:-10] + link.get('href') for link in s_links][2:-2]
        solo_page_title = [link.text.strip() for link in s_links][2:-2]

        # 处理详情页（带重试机制）
        self.process_detail_urls(solo_page_url)
        return "success"  # 分页处理成功

    def process_detail_urls(self, urls):
        """处理详情页URL，每个URL最多重试5次"""
        def detail_worker(url):
            retry_count = 0
            while retry_count < 5:
                try:
                    self.page_detail(url)
                    return
                except Exception as e:
                    print(f"详情页重试 {retry_count+1}/5: {url}")
                    retry_count += 1
                    time.sleep(2 ** retry_count)
            print(f"!! 永久跳过详情页: {url}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            executor.map(detail_worker, urls)

    def sleep(self):
        time.sleep(1)

    def page_detail(self, detail_url):
        response = requests.get(detail_url, headers=self.header).text
        soup = BeautifulSoup(response, 'html.parser')
        divs = soup.find_all('div', class_='s-prose js-post-body')

        total_p = []
        for div in divs:
            # 移除特定的 aside 标签
            asides = div.find_all('aside', class_='s-notice s-notice__info post-notice js-post-notice mb16')
            for aside in asides:
                aside.decompose()
            # print("这里是div")
            # print(div)
            # print("div结束")
            total_p.append(div)

        # 问题内容
        questions = ""
        if len(total_p) == 0 :
            return
        questions_p = total_p[0]
        # 优化后的正则表达式，匹配 <p> 标签内除 <img> 标签外的所有标签和文本
        pattern = r'</?(?!img)(?:[^>/ ]+)(?:[^>]*)>'

        for question in questions_p:
            question = str(question)
            result = re.sub(pattern, '', question)
            questions += result.replace('<p>', '').replace("</p>", '')
        questions = self.process_questions(questions)

        detail_answers = []  # 用于存储所有评论的内容和图片路径
        if len(divs) == 1:
            detail_answers.append(["None"])
        else:
            # 跳过第一个 div
            divs = divs[1:]
            for div in divs:
                answer = ""
                for p in div.find_all("p"):
                    comment_text = ""  # 初始化当前段落的文本内容
                    answer_content = str(p)
                    # 假设 pattern 是需要移除的标签模式
                    result = re.sub(pattern, '', answer_content)
                    answer += result.replace('<p>', '').replace("</p>", '').strip()
                detail_answers.append(answer)

        new_detail_answers = []
        for answer in detail_answers:
            new = self.process_questions(answer)
            new_detail_answers.append(new)
        print(len(new_detail_answers))

        # print("这是评论回复")
        # print(detail_answers)

        score_lists = []
        scores = soup.find_all("div", class_="-flair")
        # print(scores)
        for score_div in scores:
            spans = score_div.find_all("span")
            if not spans:
                continue
            score_list = []
            for span in spans:
                if "reputation-score" in span.get("class", []):
                    title = span.get("title")
                    text = span.text
                    if 'k' in text:
                        score_list.append(f"{title}")
                    else:
                        score_list.append(f"{title} {text}")
                else:
                    title = span.get("title")
                    if title:
                        score_list.append(title)
            score_lists.append(score_list)
        # print("这是评论用户的评分")
        # print(score_lists)

        edited_score = []
        # 找到所有class="user-info"的div
        edited_divs = soup.find_all("div", class_="user-info")
        for edited_div in edited_divs:
            # 检查是否存在class="user-action-time fl-grow1"，并且其中的<a>标签的文本为"edited"
            user_action_time = edited_div.find("div", class_="user-action-time fl-grow1")
            if user_action_time:
                edited_link = user_action_time.find("a")
                if edited_link and "edited" in edited_link.text.strip():
                    # 如果存在"edited"，则继续在class="user-details"下查找
                    user_details_divs = edited_div.find_all("div", class_="user-details")
                    for user_details_div in user_details_divs:
                        flair_div = user_details_div.find("div", class_="-flair")
                        if flair_div:
                            # 查找class="-flair"下面的标签，判断是否有title="reputation score"
                            for tag in flair_div.find_all():
                                if str(tag.get("title")).strip() == "reputation score":  # 使用 strip() 去除多余空格
                                    text = tag.get_text(strip=True)
                                    title = tag.get("title").strip()  # 同样去除 title 的多余空格
                                    if 'k' in text:
                                        edited_score.append(f"{title}{text}")
                                    else:
                                        edited_score.append(f"{title} {text}")
                                else:
                                    # 如果没有title="reputation score"，直接获取title
                                    title = tag.get("title")
                                    if title:
                                        edited_score.append(title)

        print(edited_score)

        # 假设soup是已经解析好的BeautifulSoup对象
        ask_score = []

        # 找到所有class="user-info"的div
        user_infos = soup.find_all("div", class_="user-info")
        for user_info in user_infos:
            # 检查是否存在class="user-action-time fl-grow1"，并且其中的文本为"asked"
            user_action_time = user_info.find("div", class_="user-action-time fl-grow1")
            if user_action_time and "asked" in user_action_time.text.strip():
                # 如果存在"asked"，则继续在class="user-details"下查找
                user_details_div = user_info.find("div", class_="user-details")
                if user_details_div:
                    flair_div = user_details_div.find("div", class_="-flair")
                    if flair_div:
                        # 查找class="-flair"下面的标签，判断是否有title="reputation score"
                        for tag in flair_div.find_all():
                            if str(tag.get("title")).strip() == "reputation score":  # 使用 strip() 去除多余空格
                                text = tag.get_text(strip=True)
                                title = tag.get("title").strip()  # 同样去除 title 的多余空格
                                if 'k' in text:
                                    ask_score.append(f"{title}{text}")
                                else:
                                    ask_score.append(f"{title} {text}")
                            else:
                                # 如果没有title="reputation score"，直接获取title
                                title = tag.get("title")
                                if title:
                                    ask_score.append(title)

        print(ask_score)

        answered_scores = []

        # 找到所有class="user-info"的div
        user_infos = soup.find_all("div", class_="user-info")
        for user_info in user_infos:
            # 检查是否存在class="user-action-time fl-grow1"，并且其中的文本为"answered"
            user_action_time = user_info.find("div", class_="user-action-time fl-grow1")
            if user_action_time and "answered" in user_action_time.text.strip():
                # 如果存在"answered"，则继续在class="user-details"下查找
                user_details_div = user_info.find("div", class_="user-details")
                if user_details_div:
                    flair_div = user_details_div.find("div", class_="-flair")
                    if flair_div:
                        user_scores = []  # 用于存储当前用户的分数信息
                        # 查找class="-flair"下面的标签，判断是否有title="reputation score"
                        for tag in flair_div.find_all():
                            if str(tag.get("title")).strip() == "reputation score":
                                text = tag.get_text(strip=True)
                                title = tag.get("title").strip()  # 去除 title 的多余空格
                                if 'k' in text:
                                    user_scores.append(f"{title}{text}")
                                else:
                                    user_scores.append(f"{title} {text}")
                            else:
                                # 如果没有title="reputation score"，直接获取title
                                title = tag.get("title")
                                if title:
                                    user_scores.append(title)
                        if user_scores:
                            answered_scores.append(user_scores)  # 将当前用户的分数信息添加到二维列表中

        # 输出结果
        print(answered_scores)

        vote_list = []
        recommendation_list = []
        vote_coulmns = soup.find_all("div",
                                     class_="js-voting-container d-flex jc-center fd-column ai-center gs4 fc-black-300")
        for vote_coulmn in vote_coulmns:
            vote_page = vote_coulmn.find_all("div",
                                             class_="js-vote-count flex--item d-flex fd-column ai-center fc-theme-body-font fw-bold fs-subheading py4")
            for vote_div in vote_page:
                vote_list.append(int(vote_div.text.strip()))
            recommendation = vote_coulmn.find_all("div",
                                                  class_="js-accepted-answer-indicator flex--item fc-green-400 py6 mtn8")
            if recommendation:
                recommendation_list.append(1)
            else:
                recommendation_list.append(0)
        print("引用列表为")
        print(recommendation_list)
        print("好评与差评列表为")
        print(vote_list)

        # 提取id
        id = detail_url.split('/')[-2]
        print(id)

        # 学科统计
        object_ul = soup.find_all("ul", class_="ml0 list-ls-none js-post-tag-list-wrapper d-inline")
        print("这是学科")
        subjects_set = set([])
        for ul in object_ul:
            lis = ul.find_all("li", class_="d-inline mr4 js-post-tag-list-item")
            for li in lis:
                a = li.find("a")
                if a:
                    subjects_set.add(a.text)
        subjects_list = list(subjects_set)
        print(subjects_list)

        # 提取问题的引用和投票信息
        question_has_reference = recommendation_list[0] if recommendation_list else 0
        question_vote_count = vote_list[0] if vote_list else 0

        # 处理评论信息
        comments = []
        for i, comment in enumerate(new_detail_answers):  # 使用 new_detail_answers 替代 detail_answers
            # 检查 answered_scores 是否有足够的长度
            commenter_score = answered_scores[i] if i < len(answered_scores) else []
            comment_info = {
                "comment": comment,  # 这里使用 new_detail_answers 中的内容
                "has_reference": recommendation_list[i + 1] if i + 1 < len(recommendation_list) else 0,
                "vote_count": vote_list[i + 1] if i + 1 < len(vote_list) else 0,
                "commenter_score": commenter_score  # 添加对应的评分信息
            }
            comments.append(comment_info)

        print("用replace处理问题内容为")
        print(questions.replace("\n", ""))
        questions = questions.replace("\n", "")
        self.sleep()

        data = {
            "id": id,
            "detail_url": detail_url,
            "subjects": subjects_list,
            "question": questions,
            "edited": edited_score,
            "asked": ask_score,
            "has_reference": question_has_reference,
            "vote_count": question_vote_count,
            "comments": comments,
        }
        # 立即写入 JSON 文件（逐行写入，支持大文件）
        self.save_single_to_json(data)
        if "physics" in detail_url:
            self.physics_num+=1
        if "chemistry" in detail_url:
            self.chemistry_num += 1
        if "math" in detail_url:
            self.math_num += 1
        if "biology" in detail_url:
            self.biology_num += 1
        print("现在已经存储的物理数据有："+str(self.physics_num)+"现在已经存储的化学数据有："+str(self.chemistry_num)+"现在已经存储的数学数据有："+str(self.math_num)+"现在已经存储的生物数据有："+str(self.biology_num))
    def download_images(self, image_links):
        if not os.path.exists('image'):
            os.mkdir('image')
        local_paths = []
        for link in image_links:
            try:
                response = requests.get(link, headers=self.header)
                if response.status_code == 200:
                    file_name = os.path.basename(link)
                    file_path = os.path.join('image', file_name)
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    print(f"成功下载图片: {file_name}")
                    local_paths.append(file_path)
                else:
                    print(f"下载图片失败，状态码: {response.status_code}")
            except Exception as e:
                print(f"下载图片时出错: {e}")
        return local_paths

    def save_single_to_json(self, data):
        """将单个数据追加到JSON数组中，确保文件始终是合法的JSON格式"""
        try:
            with self.file_lock:
                # 第一次写入时初始化文件
                if not os.path.exists(self.json_file):
                    with open(self.json_file, 'w', encoding='utf-8') as f:
                        f.write('[')  # 写入数组起始符号

                # 追加数据
                with open(self.json_file, 'r+', encoding='utf-8') as f:
                    # 移动到文件末尾
                    f.seek(0, os.SEEK_END)

                    # 如果文件不为空（已有数据）
                    if f.tell() > 1:
                        # 回退到最后一个字符（应该是 ']'）
                        f.seek(f.tell() - 1)

                        # 追加逗号（如果不是第一个元素）和新数据
                        f.write(',\n')
                        json.dump(data, f, ensure_ascii=False, indent=2)

                        # 重新写入数组结束符号
                        f.write('\n]')
                    else:
                        # 如果是第一个元素，直接写入数据和数组结束符号
                        json.dump(data, f, ensure_ascii=False, indent=2)
                        f.write('\n]')

                print(f"已保存数据: {data['id']}")

        except Exception as e:
            print(f"保存单条数据时出错: {e}")

# if __name__ == '__main__':
#     spider_instance = spider()  # 创建 Spider 实例
#      base_url_list = [
#         "https://math.stackexchange.com/questions",
#         "https://physics.stackexchange.com/questions",
#         "https://biology.stackexchange.com/questions/",
#         "https://chemistry.stackexchange.com/questions"
#     ]
#     tab = "newest"  # 排序方式
#     # 循环生成并处理每个分页的 URL
#     for base_url in base_url_list:
#         first_base_url = base_url
#         for page in range(1, 100000000000000000000000000000000000000000):
#             url = f"{base_url}?tab={tab}&page={page}"
#             print(url)
#             return_result = spider_instance.page_url(url,first_base_url)  # 调用方法处理每个 URL
#             if return_result == 0:
#                 break


if __name__ == '__main__':
    spider = spider()
    base_url_list = [
                "https://biology.stackexchange.com/questions/",
            ]
    tab = "newest"
    for base_url in base_url_list:
        page = 1
        spider.count = 0  # 重置站点计数器

        while True:
            # 分页处理（每个page最多尝试5次）
            page_retry = 0
            page_result = None

            while page_retry < 5:
                current_url = f"{base_url}?tab={tab}&page={page}"
                print(f"处理分页: {current_url} (尝试 {page_retry + 1}/5)")

                result = spider.page_url(current_url, base_url)

                if result == 5:
                    print(">> 达到连续5次无效页面，切换站点 <<")
                    break
                elif result == "success":
                    print("分页处理成功，跳转下一页")
                    page += 1
                    page_result = "success"
                    break
                elif result == "invalid_page":
                    print("当前页面无效，保持页码重试")
                    page_retry += 1
                    time.sleep(3)
                else:  # retry_page
                    print("网络错误，重试当前页")
                    page_retry += 1
                    time.sleep(5)

            # 分页处理结果判断
            if result == 5:
                break  # 切换站点
            elif page_result != "success":
                print("!! 分页重试5次未成功，强制下一页 !!")
                page += 1
                spider.count = 0  # 重置连续失败计数

            # 安全保护：避免无限翻页
            if page > 2000:
                print("!! 达到最大翻页限制，切换站点 !!")
                break