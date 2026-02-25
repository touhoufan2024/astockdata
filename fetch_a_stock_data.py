#!/usr/bin/env python
# -*- coding: utf-8 -*-

import akshare as ak
from openai import OpenAI
import pandas as pd
import os
import fire
from loguru import logger
import inspect
import time
import random
from pathlib import Path
from datetime import datetime

# 1. 获取当前时间
now = datetime.now()
# 2. 格式化为：年-月-日_时-分 (例如: 2026-02-25_16-10)
timestamp = now.strftime("%Y-%m-%d_%H-%M")
api_key = os.getenv("DS_API_KEY")


a_stock_dir = "./a_stock_dir"

class FetchData:
    def _add_exchange_prefix(self, symbol):
        if symbol.startswith(('6', '9')):
            return f"sh{symbol}"
        elif symbol.startswith(('0', '3', '2')):
            return f"sz{symbol}"
        elif symbol.startswith(('4', '8')):
            return f"bj{symbol}"
        return symbol

    def _get_func_name(self):
        """核心包裝方法：獲取『呼叫者』的名稱"""
        # f_back 代表回到上一層呼叫者的框架
        return inspect.currentframe().f_back.f_code.co_name

    def _get_csi300_codes(self):
        return ak.index_stock_cons_csindex(symbol="000300")["成分券代码"].unique()
    def _get_csi500_codes(self):
        return ak.index_stock_cons_csindex(symbol="000500")["成分券代码"].unique()

    def my_stock_zh_index_daily(self):
        func_name = self._get_func_name()
        logger.info(f"executing function : {func_name}")
        csi300_codes = self._get_csi300_codes()
        _dir = f"{a_stock_dir}/{func_name}"
        os.makedirs(_dir, exist_ok=True)
        # 設定參數
        max_retries = 3  # 最多重試 3 次
        retry_delay = 30 # 發生異常時休眠 30 秒

        for index, code in enumerate(csi300_codes, start=1):
            logger.info(f"Fetching data for {code} ({index}/{len(csi300_codes)})")
            symbol = self._add_exchange_prefix(code)
            file_path = f"{_dir}/{symbol}.csv"

            # 1. 檢查檔案是否已存在 (斷點續傳)
            if os.path.exists(file_path):
                logger.info(f"{symbol} data already exists, skipping...")
                continue

            # 2. 開始抓取邏輯，加入重試機制
            retries = 0
            success = False

            while retries < max_retries and not success:
                try:
                    logger.info(f"Fetching data for {symbol} (Attempt {retries + 1})")

                    # 執行抓取
                    _df = ak.stock_zh_index_daily(symbol=symbol)

                    # 保存檔案
                    _df.to_csv(file_path, index=False)

                    # 成功後標記並給予一個隨機的小休眠，防止請求過快
                    success = True
                    time.sleep(random.uniform(1.0, 2.0))

                except Exception as e:
                    retries += 1
                    logger.error(f"Error fetching {symbol}: {e}")

                    if retries < max_retries:
                        logger.info(f"Sleeping for {retry_delay} seconds before retrying...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Max retries reached for {symbol}. Moving to next code.")


    def _llm_analyze_entry(self, prompt):
        # 初始化客户端
        client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com" # 必须指定为 DeepSeek 的地址
        )

        # 发送问题
        response = client.chat.completions.create(
            model="deepseek-chat",  # 或者使用 "deepseek-reasoner"
            messages=[
                {"role": "system", "content": "你是一个专业的量化分析助手。"},
                {"role": "user", "content": prompt}
            ],
            stream=False  # 设置为 True 可以实现打字机流式效果
        )

        return response.choices[0].message.content

    def llm_analyze(self):
        func_name = self._get_func_name()
        logger.info(f"executing function : {func_name}")
        _dir = f"{a_stock_dir}/{func_name}_{timestamp}"
        os.makedirs(_dir, exist_ok=True)


        data_dir = Path(f"{a_stock_dir}/my_stock_zh_index_daily")
        for i, csv_file in enumerate(data_dir.glob("*.csv"), start=1):
            logger.info(f"[{i}] 正在处理文件: {csv_file.name}")

            # 直接读取
            _df = pd.read_csv(csv_file)
            recent = _df.tail(60)[['date', 'open', 'high', 'low', 'close', 'volume']]
            data_str = recent.to_string(index=False)

            prompt = f"请分析{csv_file.stem}最近60个交易日的走势：\n\n{data_str}\n\n请给出趋势研判和支撑压力位。并给出空仓和持仓时的操作建议, 在开头给出最新日期"
            # ret = self._llm_analyze_entry(prompt)
            ret = "test response"

            _md_dir = f"{_dir}/{csv_file.stem}.md"
            with open(_md_dir, "w", encoding="utf-8") as f:
                f.write(_ret)

if __name__ == '__main__':
    fire.Fire(FetchData)
