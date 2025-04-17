from .base import base_crawler
import argparse
import time
import os
import sys
import requests
import base64
import psycopg2
import cairosvg
import pandas as pd
import re

from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementNotInteractableException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from io import BytesIO
from PIL import Image
from psycopg2 import sql
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pathlib import Path
from psycopg2.extras import DictCursor


class crawler_hoaddondientu(base_crawler):
    # Class level CONSTANT
    # Tạo bảng nếu chưa tồn tại
    CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS data_hoadon (
        id SERIAL PRIMARY KEY,
        mau_so VARCHAR(255),
        ky_hieu VARCHAR(255),
        so_hoa_don VARCHAR(255),
        ngay_lap DATE,
        mst_nguoi_mua VARCHAR(255),  -- Mã số thuế người mua
        ten_nguoi_mua TEXT,  -- Tên người mua
        mst_nguoi_ban VARCHAR(255),  -- Mã số thuế người bán
        ten_nguoi_ban TEXT,  -- Tên người bán
        tong_tien_chua_thue VARCHAR,
        tong_tien_thue VARCHAR,
        tong_tien_chiet_khau VARCHAR,
        tong_tien_phi VARCHAR,
        tong_tien_thanh_toan VARCHAR,
        don_vi_tien_te VARCHAR(255),
        trang_thai VARCHAR(255),
        image_drive_path VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        company_id VARCHAR(255),
        company_name VARCHAR(255),
        loai_hoa_don VARCHAR(100), -- Thêm cột loại hóa đơn
        UNIQUE (company_id, so_hoa_don, loai_hoa_don) 
    );
    """
    # Tạo khóa ngoại nếu chưa tồn tại
    ADD_FOREIGN_KEY_QUERY = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 
            FROM information_schema.table_constraints 
            WHERE constraint_name = 'fk_company_id' 
            AND table_name = 'data_hoadon'
        ) THEN
            ALTER TABLE data_hoadon 
            ADD CONSTRAINT fk_company_id
            FOREIGN KEY (company_id) 
            REFERENCES company_information (company_id);
        END IF;
    END $$;
    """

    def __init__(self):
        # Use init from base class
        super().__init__()

    def parse_arguments(self):
        """Parse command line arguments with environment variables as defaults."""
        parser = argparse.ArgumentParser(description="Hóa đơn điện tử Data Crawler")
        current_date = datetime.now()
        parser.add_argument(
            "--months-ago",
            type=int,
            default=0,
            required=False,
            help="Số tháng cần quay lại từ tháng hiện tại. "
            "0: Tháng hiện tại, "
            "1: Lùi về 1 tháng",
        )

        parser.add_argument(
            "--crawl-months",
            type=int,
            default=1,
            required=False,
            help="Số lượng tháng muốn crawl. Mặc định: 1 tháng",
        )
        parser.add_argument("--month", default=str(current_date.month), required=False, help="Tháng cần crawl (1-12)")
        parser.add_argument("--year", type=str, required=False, default=str(current_date.year), help="Năm cần tra cứu (1990-hiện tại)")
        parser.add_argument("--company", default=None, required=False, help="Tên công ty cần crawl.")

        self.args = parser.parse_args()

        if self.args.months_ago < 0:
            print(
                "[WARNING] Giá trị months-ago không được âm. Đặt về 0. Chỉ lấy tháng hiện tại."
            )
            self.args.months_ago = 0

        if self.args.crawl_months < 1 or self.args.months_ago == 0:
            print("[WARNING] Số tháng crawl phải lớn hơn 0. Đặt về 1 tháng.")
            self.args.crawl_months = 1

        return self.args

    # 1.1 Nhập username và password vào trang web 'hoadondientu'
    def login_to_hoadondientu(self, driver, username, password, company_id, company_name):
        """Đăng nhập vào trang web 'hoadondientu'."""

        url = "https://hoadondientu.gdt.gov.vn/"
        driver.get(url)
        print("- Finish initializing a driver")
        self.send_slack_notification(
            f"[INFO] Chương trình đang login vào công ty có id: {company_id} - tên {company_name}", self.webhook_url_hddt
        )
        time.sleep(3)

        try:
            X_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "/html/body/div[2]/div/div[2]/div/div[2]/button/span")
                )
            )                   
            X_button.click()
            print("[SUCCESS] - Finish task 1: Tắt thông báo")
        except TimeoutException:
            print("[DEBUG] X_button không hiển thị hoặc không thể nhấn")
            pass
        except Exception as e:
            print("[ERROR] Đã xảy ra lỗi: X_button click failed")

        # Nhấn nút logout
        try:
            logout_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="__next"]/section/header/div[2]/button[2]')
                )
            )
            logout_button.click()
            print("[SUCCESS] - Finish: logout to hoadondientu")
        except TimeoutException:
            print("[DEBUG] logout_button không hiển thị hoặc không thể nhấn")
            pass

        # Nhấn nút Đăng nhập
        try:
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//div[contains(@class, 'home-header-menu')]//div[contains(@class, 'ant-col home-header-menu-item')]//span[text()='Đăng nhập']",
                    )
                )
            )
            login_button.click()
            print("[SUCCESS] - Finish task 2: Login to hoadondientu")
        except TimeoutException:
            print("[DEBUG] Login button không hiển thị hoặc không thể nhấn")
        # Nhập username
        username_field = driver.find_element(By.ID, "username")
        username_field.send_keys(username)
        print("[SUCCESS] - Finish keying in username_field")
        print(f"- Username_field: {username}")
        time.sleep(3)

        # Nhập password
        password_field = driver.find_element(By.ID, "password")
        password_field.send_keys(password)
        print("[SUCCESS] - Finish keying in password_field")
        time.sleep(2)

    # lưu ảnh captcha về máy dưới dạng svg (tải ảnh về chuẩn rồi)
    def crawl_img(self, driver):
        try:
            # Tìm phần tử img chứa ảnh captcha
            img = driver.find_element(
                By.CLASS_NAME, "Captcha__Image-sc-1up1k1e-1.kwfLHT"
            )

            # Lấy giá trị của thuộc tính 'src' của thẻ img
            img_src = img.get_attribute("src")

            # Kiểm tra nếu src bắt đầu bằng 'data:image/svg+xml;base64,' (đặc trưng của ảnh base64)
            if img_src.startswith("data:image/svg+xml;base64,"):
                # Loại bỏ phần 'data:image/svg+xml;base64,' từ chuỗi base64
                base64_data = img_src.split("data:image/svg+xml;base64,")[1]

                # Giải mã base64 thành dữ liệu nhị phân
                img_data = base64.b64decode(base64_data)

                # Tạo tên file cho ảnh (có thể thay đổi theo nhu cầu)
                file_name = "captcha_image.svg"

                # Lưu ảnh dưới dạng file SVG
                with open(file_name, "wb") as f:
                    f.write(img_data)

                print(f"[SUCCESS] Ảnh đã được tải về và lưu thành công với tên: {file_name}")

            else:
                print("[ERROR] Không tìm thấy ảnh SVG base64 trong src của thẻ img.")
                self.send_slack_notification(
                    "[ERROR] Workflow crawling data hoadondientu failed",
                    self.webhook_url_hddt,
                )

        except Exception as e:
            print("[ERROR] Đã xảy ra lỗi: lưu ảnh captcha về máy")
            self.send_slack_notification(
                "[ERROR] Workflow crawling data hoadondientu failed", self.webhook_url_hddt
            )

    # Hàm gửi ảnh đến AntiCaptcha
    def solve_captcha(self, image_base64):
        url = "https://anticaptcha.top/api/captcha"
        payload = {
            "apikey": self.api_key_anticaptcha,
            "img": image_base64,
            "type": 28,  # Loại captcha, có thể cần thay đổi nếu không đúng
        }
        headers = {"Content-Type": "application/json"}

        try:
            # Gửi POST request
            response = requests.post(url, json=payload, headers=headers)

            # Kiểm tra nếu có lỗi trong phản hồi HTTP
            if response.status_code != 200:
                print(f"Error with request: {response.status_code}")
                print(f"Response Text: {response.text}")
                return None

            # Phân tích phản hồi JSON
            response_data = response.json()

            # Kiểm tra xem API trả về thành công
            if response_data.get("success") and "captcha" in response_data:
                print(f"[INFO] Mã captcha đã giải: {response_data['captcha']}")
                return response_data["captcha"]
            else:
                print(f"[INFO] API response indicates failure: {response_data}")
                self.send_slack_notification(
                    f"[ERROR] Workflow crawling data hoadondientu failed {response_data}",
                    self.webhook_url_hddt,
                )
                return None
        except Exception as e:
            print("[ERROR] Error with request: gửi ảnh đến AntiCaptcha")
            self.send_slack_notification(
                "[ERROR] Workflow crawling data hoadondientu failed", self.webhook_url_hddt
            )
            return None

    # Hàm xử lý ảnh captcha và gửi lên AntiCaptcha
    def solve_captcha_from_file(self, file_path):
        try:
            # Đọc file captcha
            with open(file_path, "rb") as file:
                # Kiểm tra nếu file là SVG
                if file_path.endswith(".svg"):
                    # Đọc nội dung của file SVG
                    svg_content = file.read()

                    # Chuyển đổi file SVG thành PNG
                    png_bytes = cairosvg.svg2png(bytestring=svg_content)

                    # Mã hóa ảnh PNG thành base64
                    image_base64 = base64.b64encode(png_bytes).decode("utf-8")
                else:
                    # Nếu là ảnh raster (PNG, JPEG), chuyển sang PNG và mã hóa base64
                    img = Image.open(file)
                    buffered = BytesIO()
                    img.save(buffered, format="PNG")
                    image_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")

            captcha_text = self.solve_captcha(image_base64)

            # Trả về mã captcha đã giải, không in ra nhiều lần
            return captcha_text
        except Exception as e:
            print(f"[ERROR] An error occurred: xử lý ảnh captcha và gửi lên")
            self.send_slack_notification(
                "[ERROR] Workflow crawling data hoadondientu failed", self.webhook_url_hddt
            )
            return None

    # 1.2 Nhập mã Captcha (tự động)
    def enter_verification_code(self, driver, captcha_image_path):
        try:
            # Giải mã captcha bằng hàm solve_captcha_from_file
            captcha_code = self.solve_captcha_from_file(captcha_image_path)
            if not captcha_code:
                print("[ERROR] Không thể giải mã captcha.")
                sys.exit(1)  # Thoát chương trình
                return False

            # Tìm tất cả phần tử có id 'cvalue'
            elements = driver.find_elements(By.ID, "cvalue")
            print(f"[DEBUG] Số phần tử với id='cvalue': {len(elements)}")

            # Nếu có nhiều hơn một phần tử, chọn phần tử cụ thể
            if len(elements) > 1:
                captcha_field = elements[1]
            else:
                captcha_field = elements[0]

            # Nhập CAPTCHA
            captcha_field.clear()
            captcha_field.send_keys(captcha_code)
            time.sleep(2)

            # Log giá trị sau khi nhập
            captcha_value = captcha_field.get_attribute("value")
            print(f"[DEBUG] Giá trị CAPTCHA sau khi nhập: {captcha_value}")

            return True
        except Exception as e:
            print(f"[ERROR] Lỗi khi nhập mã CAPTCHA trên website")
            return False

    # 1.3 Nhấn nút đăng nhập sau cùng hoàn tất việc login vào trang web
    def submit_form(self, driver, captcha_image_path):
        """Nhấn nút để hoàn tất đăng nhập."""
        login_attempt = 0  # Biến đếm số lần đăng nhập
        max_attempts = 3  # Giới hạn số lần thử tối đa

        try:
            while login_attempt < max_attempts:
                # Nhấn nút để gửi biểu mẫu
                login_attempt +=1
                
                submit_button = driver.find_element(
                    By.XPATH,
                    "/html/body/div[2]/div/div[2]/div/div[2]/div[2]/form/div/div[6]/button",
                )
                submit_button.click()
                print(f"[DEBUG] - Finish submitting the form (Lần {login_attempt})")
                self.send_slack_notification(
                    f"[INFO] Chương trình đang thực hiên login lần {login_attempt}",
                    self.webhook_url_hddt,
                )
                
                # Kiểm tra nếu có thông báo lỗi CAPTCHA
                try:
                    # Chờ thông báo lỗi CAPTCHA
                    error_message = WebDriverWait(driver, 2).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//*[contains(text(), "Mã captcha không đúng.")]')
                        )
                    )
                    if error_message:
                        print(f"[ERROR] Mã xác thực không chính xác (Lần {login_attempt}/{max_attempts})")
                        self.send_slack_notification(
                            "[WARNING] Login thất bại. Đang thử lại...",
                            self.webhook_url_hddt,
                        )
                        if login_attempt >= max_attempts:
                            print("[ERROR] Quá số lần thử, bỏ qua công ty này.")
                            raise Exception("[ERROR] Quá số lần thử đăng nhập, bỏ qua công ty.")
                        
                        # Lưu CAPTCHA mới và giải mã CAPTCHA mới
                        self.crawl_img(driver)
                        self.enter_verification_code(driver, captcha_image_path)
                        continue  # Thử lại
                    
                except TimeoutException:
                    print("[DEBUG] Mã xác nhận được xác thực thành công")
                    
                # Kiểm tra nếu có lỗi tên đăng nhập hoặc mật khẩu sai
                try:
                    error_message = WebDriverWait(driver, 2).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//*[contains(text(), "Tên đăng nhập hoặc mật khẩu không đúng")]')
                        )
                    )
                    if error_message:
                        print(f"[ERROR] có thông báo Tên đăng nhập hoặc mật khẩu không đúng.(Lần {login_attempt}/{max_attempts})")
                        self.send_slack_notification(
                            "[WARNING] Login thất bại. Đang thử lại...",
                            self.webhook_url_hddt,
                        )
                        if login_attempt >= max_attempts:
                            print("[ERROR] Quá số lần thử, bỏ qua công ty này.")
                            raise Exception("[ERROR] Quá số lần thử đăng nhập, bỏ qua công ty.")
                        
                        # Lưu CAPTCHA mới và giải mã CAPTCHA mới
                        self.crawl_img(driver)
                        self.enter_verification_code(driver, captcha_image_path)
                        continue  # Thử lại
                        
                except TimeoutException:
                    print("[DEBUG] Thông tin đăng nhập hợp lệ, tiếp tục kiểm tra đăng nhập thành công.")
                    
                # Kiểm tra nếu có nút logout thì đăng nhập thành công
                try:
                    X_button = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable(
                            (By.XPATH, "/html/body/div[1]/section/header/div[2]/button[2]")
                        )               
                    )
                    
                    if X_button:
                        print(
                            "[SUCCESS] Chương trình đã login thành công vào trang HDDT"
                        )
                        self.send_slack_notification(
                            "[SUCCESS] Đăng nhập thành công! Đã vào trang chính.",
                            self.webhook_url_hddt,
                        )
                        if login_attempt == 1:
                            self.crawl(driver)  # Lần đầu tiên, gọi hàm crawl
                        else:
                            self.crawls(driver)  # Các lần tiếp theo, gọi hàm crawls
                        return  # Thoát khỏi hàm khi thành công
                except TimeoutException:
                    print(f"[DEBUG] Không tìm thấy dấu hiệu đăng nhập thành công (Lần {login_attempt}/{max_attempts})")
                
                # Nếu đến đây nghĩa là sau `max_attempts` lần vẫn chưa đăng nhập được
            print("[ERROR] Đăng nhập thất bại sau 3 lần thử, bỏ qua công ty này.")
            raise Exception("[ERROR] Đăng nhập thất bại sau 3 lần thử.")

                # Nếu không vào được vòng lặp, thoát ra
        except Exception as e:
            print(f"[ERROR] Đã xảy ra lỗi khi nhấn nút submit khi login:")
            self.send_slack_notification(
                "[FAILED] Chương trình chạy thất bại", self.webhook_url_hddt
            )

    # 2.1 chọn vào mục ( Tra cứu hóa đơn ) khi giải captcha lần đầu thành công
    def crawl(self, driver):
        # Nhấn nút tra cứu
        tra_cuu_button = driver.find_element(
            By.XPATH,
            '/html/body/div/section/section/div/div/div/div/div[8]/div/span',
        )
        tra_cuu_button.click()
        print("[SUCCESS] - Finish click tra cứu")
        time.sleep(3)

        # Chọn vào mục ( Tra cứu hóa đơn )
        tra_cuu_hd_button = driver.find_element(
            By.XPATH, "/html/body/div[2]/div/div/ul/li[1]/a"
        )               
        tra_cuu_hd_button.click()
        print("[SUCCESS] - Finish click tra cứu hóa đơn")
        time.sleep(3)

    # 2.2 chọn vào mục ( Tra cứu hóa đơn ) khi giải captcha các lần sau thành công
    def crawls(self, driver):
        # Nhấn nút tra cứu
        tra_cuu_button = driver.find_element(
            By.XPATH,
            '/html/body/div[1]/section/section/div/div/div/div/div[8]/div/span',
        )
        
        tra_cuu_button.click()
        print("[SUCCESS] - Finish click tra cứu")
        time.sleep(3)

        # Chọn vào mục ( Tra cứu hóa đơn )
        tra_cuu_hd_button = driver.find_element(
            By.XPATH, "/html/body/div[3]/div/div/ul/li[1]/a"
        )
        tra_cuu_hd_button.click()
        print("[SUCCESS] - Finish click tra cứu hóa đơn")
        time.sleep(3)

    def navigate_to_first_day_of_month(self, driver, months_to_go_back=0):
        """Navigate to the first day of the month."""
        try:
            # Wait for the previous month button to be clickable
            prev_month_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '//a[@class="ant-calendar-prev-month-btn"]')
                )
            )

            # Click previous month button the specified number of times
            for _ in range(months_to_go_back):
                prev_month_button.click()
                time.sleep(0.5)

            # Find and click day 1
            first_row_days = driver.find_elements(
                By.XPATH,
                '//div[contains(@class, "ant-calendar-date-panel")]//tr[1]/td/div',
            )
            for day in first_row_days:
                if day.text.strip() == "1":
                    day.click()
                    print(
                        f"[DEBUG] - Navigated to first day of the month, went back {months_to_go_back} months"
                    )
                    return True

            raise Exception("[DEBUG] Could not find day '1' in the first row")

        except Exception as e:
            print("[WARNING] Failed to navigate to first day continue")
            return False

    # 3. chọn vào tab ( - Tra cứu hóa đơn điện tử mua vào - ) để crawl dữ liệu
    def crawl_hoa_don_mua_vao(self, driver):
        # Chọn Tra cứu hóa đơn điện tử mua vào
        mua_vao_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    '//*[@id="__next"]/section/section/main/div/div/div/div/div[1]/div/div/div/div/div[1]/div[2]/span',
                )
            )
        )
        mua_vao_button.click()
        print("[SUCCESS]- Finish click tab tra cứu hóa đơn mua vào")
        time.sleep(3)

        try:
            # Chờ cho các thẻ input xuất hiện
            inputs = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, '//*[@id="tngay"]/div/input')
                )
            )

            # Chọn thẻ input ở vị trí thứ 3
            target_input_to = inputs[1]

            target_input_to.click()
            print("[SUCCESS]- click thành công vào input")

            # Chỉ truyền số tháng cần lùi
            self.navigate_to_first_day_of_month(
                driver, months_to_go_back=self.args.months_ago
            )
            print("[SUCCESS] - Đã chọn thời gian tìm kiếm.")

        except Exception as e:
            print(f"[ERROR] Gặp lỗi khi thao tác với thẻ input: Tra cứu hóa đơn điện tử mua vào ")
            self.send_slack_notification(
                "[ERROR]Chương trình chạy thất bại", self.webhook_url_hddt
            )
        # Click vào Kết quả kiểm tra: (Đã cấp mã hóa đơn)
        ket_qua_kiem_tra_1 = driver.find_element(
            By.XPATH, '/html/body/div[1]/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[1]/div/div/form/div[1]/div[5]/div/div[2]/div/div/div/span/div/div/div',
        )
        ket_qua_kiem_tra_1.click()
        print("[SUCCESS]- Finish click vào dropdown ket_qua_kiem_tra_1")
        time.sleep(2)
        
        # # Click vào item "Đã cấp mã hóa đơn"
        # item_to_select = driver.find_element(
        #     By.XPATH, '//div[contains(text(), "Đã cấp mã hóa đơn")]'
        # )
        # item_to_select.click()
        # print("[SUCCESS] - Đã chọn 'Đã cấp mã hóa đơn'")
        # time.sleep(2)
        
        # Đợi dropdown thực tế render ra và chứa option mong muốn
        wait = WebDriverWait(driver, 10)
        option = wait.until(EC.element_to_be_clickable((
            By.XPATH,
            '//div[contains(@class, "ant-select-dropdown") and contains(@class, "ant-select-dropdown--single")]//li[contains(text(), "Đã cấp mã hóa đơn")]'
        )))
        option.click()
        print("[SUCCESS] - Đã chọn 'Đã cấp mã hóa đơn'")
        
        
        # Chọn nút Tìm kiếm
        tim_kiem = driver.find_element(
            By.XPATH,
            '//*[@id="__next"]/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[1]/div/div/form/div[3]/div[1]/button',
        )
        tim_kiem.click()
        print("[SUCCESS]- Finish click tìm kiếm hóa đơn mua vào")
        time.sleep(2)
        
    # 3.1 chọn vào filter (Cục thuế đã nhận không mã) ở tab ( - Tra cứu hóa đơn điện tử mua vào - ) để crawl dữ liệu
    def crawl_hoa_don_mua_vao_filter(self, driver):
        
        # Click vào Kết quả kiểm tra: (Cục Thuế đã nhận không mã)
        ket_qua_kiem_tra_2 = driver.find_element(
            By.XPATH, '/html/body/div[1]/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[1]/div/div/form/div[1]/div[5]/div/div[2]/div/div/div/span/div/div/div',
        )
        ket_qua_kiem_tra_2.click()
        print("[SUCCESS]- Finish click vào dropdown ket_qua_kiem_tra_2")
        time.sleep(2)
        
        # Đợi dropdown thực tế render ra và chứa option mong muốn
        wait = WebDriverWait(driver, 10)
        option = wait.until(EC.element_to_be_clickable((
            By.XPATH,
            '//div[contains(@class, "ant-select-dropdown") and contains(@class, "ant-select-dropdown--single")]//li[contains(text(), "Cục Thuế đã nhận không mã")]'
        )))
        option.click()
        print("[SUCCESS] - Đã chọn 'Cục Thuế đã nhận không mã'")
        
        # wait = WebDriverWait(driver, 3)
        # option = wait.until(EC.element_to_be_clickable(
        #     (By.XPATH, '//div[@class="ant-select-selection-selected-value" and @title="Cục Thuế đã nhận không mã"]')
        # ))
        # option.click()
        # print("[SUCCESS] - Đã click vào thẻ có nội dung 'Cục Thuế đã nhận không mã'")
        # time.sleep(2)
        

        # Chọn nút Tìm kiếm
        tim_kiem = driver.find_element(
            By.XPATH,
            '//*[@id="__next"]/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[1]/div/div/form/div[3]/div[1]/button',
        )
        tim_kiem.click()
        print("[SUCCESS]- Finish click tìm kiếm hóa đơn mua vào")
        time.sleep(2)


    # ( Hàm Thêm stt sau mỗi file trùng tên )
    def get_unique_filename(self, base_filename):
        if not os.path.exists(base_filename):
            return base_filename

        base, ext = os.path.splitext(base_filename)
        counter = 1
        new_filename = f"{base} ({counter}){ext}"

        while os.path.exists(new_filename):
            counter += 1
            new_filename = f"{base} ({counter}){ext}"

        return new_filename

    
    
    # Task 4 xuất các hàng dữ liệu ở trang ( - Tra cứu hóa đơn điện tử mua vào - ) ra file csv
    def extract_table_mua_vao_to_csv(self, driver, output_file):
        """Lấy dữ liệu từ bảng ngang có thanh cuộn và lưu vào file CSV."""
        try:
            # Tạo tên file duy nhất nếu cần
            unique_output_file = self.get_unique_filename(output_file)
            # Chờ bảng hiển thị
            table1 = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        '//*[@id="__next"]/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[2]/div[2]/div[3]/div[1]/div[2]/div/div/div/div/div/div[1]/table',
                    )
                )
            )
            # Tìm thanh cuộn ngang
            scrollable_div = driver.find_element(
                By.XPATH,
                "/html/body/div/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[2]/div[2]/div[3]/div[1]/div[2]/div/div/div/div/div/div[2]",
            )

            # Lấy chiều rộng cuộn tối đa
            max_scroll_width = driver.execute_script(
                "return arguments[0].scrollWidth;", scrollable_div
            )
            current_scroll_position = 0
            scroll_step = 500  # Số pixel cuộn ngang mỗi lần

            # Khởi tạo lưu trữ dữ liệu
            all_headers = []
            all_rows = []

            while current_scroll_position < max_scroll_width:
                # Lấy HTML hiện tại của bảng có thead class 'ant-table-thead'
                table_html = table1.get_attribute("outerHTML")
                soup = BeautifulSoup(table_html, "html.parser")

                # Lấy tiêu đề nếu tồn tại
                header_row = soup.find("thead")
                if header_row:
                    header_columns = header_row.find_all("th")
                    headers = [header.text.strip() for header in header_columns]
                    # Chỉ thêm các tiêu đề mới
                    if not all_headers:
                        all_headers = headers
                    elif len(headers) > len(all_headers):
                        all_headers += headers[
                            len(all_headers) :
                        ]  # Thêm cột mới vào cuối
                else:
                    print("[DEBUG] Không tìm thấy tiêu đề bảng.")

                # Lấy dữ liệu từ tbody
                # Tìm tất cả phần tử có class 'ant-table-tbody'
                elements2 = driver.find_elements(By.CLASS_NAME, "ant-table-tbody")
                # print(f"[DEBUG] Số phần tử với class='ant-table-body': {len(elements2)}")

                # Chọn phần tử thứ hai (index 1)
                if len(elements2) > 1:
                    tbody = elements2[1]
                else:
                    raise Exception("[DEBUG] Không tìm thấy phần tử ant-table-body thứ hai.")

                # Lấy tất cả các hàng hiện tại
                rowsbody = tbody.find_elements(By.XPATH, ".//tr")
                # Duyệt qua các hàng
                for row in rowsbody:
                    cols = row.find_elements(By.XPATH, "./td")
                    row_data = [col.text.strip() for col in cols]
                    # Đảm bảo chiều dài hàng phù hợp với số cột
                    while len(row_data) < len(all_headers):
                        row_data.append("")  # Thêm ô trống
                    all_rows.append(row_data)

                # Cuộn thanh cuộn ngang
                current_scroll_position += scroll_step
                driver.execute_script(
                    f"arguments[0].scrollLeft = {current_scroll_position};",
                    scrollable_div,
                )
                time.sleep(1)

                # Kiểm tra cuộn xong chưa
                new_scroll_position = driver.execute_script(
                    "return arguments[0].scrollLeft;", scrollable_div
                )
                if new_scroll_position == current_scroll_position:
                    break

            # Lưu vào DataFrame
            if not all_headers:
                print("[ERROR] Không tìm thấy tiêu đề để tạo DataFrame.")
                return

            df = pd.DataFrame(all_rows, columns=all_headers)
            df.to_csv(unique_output_file, index=False, encoding="utf-8-sig")
            print(f"[SUCCESS]- Dữ liệu đã được lưu vào file: {unique_output_file}")

        except Exception as e:
            print(f"[ERROR] Không thể lấy dữ liệu từ bảng: {e}")
            self.send_slack_notification(
                "[FAILED] Chương trình chạy thất bại", self.webhook_url_hddt
            )
    
    
    # Task 4.1 xuất các hàng dữ liệu filter ở trang ( - Tra cứu hóa đơn điện tử mua vào - ) ra file csv        
    def extract_table_mua_vao_to_csv_filter(self, driver, output_file_filter):
        """Lấy dữ liệu từ bảng ngang có thanh cuộn và lưu vào file CSV."""
        try:
            # Tạo tên file duy nhất nếu cần
            unique_output_file = self.get_unique_filename(output_file_filter)
            # Chờ bảng hiển thị
            table1 = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        '//*[@id="__next"]/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[2]/div[2]/div[3]/div[1]/div[2]/div/div/div/div/div/div[1]/table',
                    )
                )
            )
            # Tìm thanh cuộn ngang
            scrollable_div = driver.find_element(
                By.XPATH,
                "/html/body/div/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[2]/div[2]/div[3]/div[1]/div[2]/div/div/div/div/div/div[2]",
            )

            # Lấy chiều rộng cuộn tối đa
            max_scroll_width = driver.execute_script(
                "return arguments[0].scrollWidth;", scrollable_div
            )
            current_scroll_position = 0
            scroll_step = 500  # Số pixel cuộn ngang mỗi lần

            # Khởi tạo lưu trữ dữ liệu
            all_headers = []
            all_rows = []

            while current_scroll_position < max_scroll_width:
                # Lấy HTML hiện tại của bảng có thead class 'ant-table-thead'
                table_html = table1.get_attribute("outerHTML")
                soup = BeautifulSoup(table_html, "html.parser")

                # Lấy tiêu đề nếu tồn tại
                header_row = soup.find("thead")
                if header_row:
                    header_columns = header_row.find_all("th")
                    headers = [header.text.strip() for header in header_columns]
                    # Chỉ thêm các tiêu đề mới
                    if not all_headers:
                        all_headers = headers
                    elif len(headers) > len(all_headers):
                        all_headers += headers[
                            len(all_headers) :
                        ]  # Thêm cột mới vào cuối
                else:
                    print("[DEBUG] Không tìm thấy tiêu đề bảng.")

                # Lấy dữ liệu từ tbody
                # Tìm tất cả phần tử có class 'ant-table-tbody'
                elements2 = driver.find_elements(By.CLASS_NAME, "ant-table-tbody")
                # print(f"[DEBUG] Số phần tử với class='ant-table-body': {len(elements2)}")

                # Chọn phần tử thứ hai (index 1)
                if len(elements2) > 1:
                    tbody = elements2[1]
                else:
                    raise Exception("[DEBUG] Không tìm thấy phần tử ant-table-body thứ hai.")

                # Lấy tất cả các hàng hiện tại
                rowsbody = tbody.find_elements(By.XPATH, ".//tr")
                # Duyệt qua các hàng
                for row in rowsbody:
                    cols = row.find_elements(By.XPATH, "./td")
                    row_data = [col.text.strip() for col in cols]
                    # Đảm bảo chiều dài hàng phù hợp với số cột
                    while len(row_data) < len(all_headers):
                        row_data.append("")  # Thêm ô trống
                    all_rows.append(row_data)

                # Cuộn thanh cuộn ngang
                current_scroll_position += scroll_step
                driver.execute_script(
                    f"arguments[0].scrollLeft = {current_scroll_position};",
                    scrollable_div,
                )
                time.sleep(1)

                # Kiểm tra cuộn xong chưa
                new_scroll_position = driver.execute_script(
                    "return arguments[0].scrollLeft;", scrollable_div
                )
                if new_scroll_position == current_scroll_position:
                    break

            # Lưu vào DataFrame
            if not all_headers:
                print("[ERROR] Không tìm thấy tiêu đề để tạo DataFrame.")
                return

            df = pd.DataFrame(all_rows, columns=all_headers)
            df.to_csv(unique_output_file, index=False, encoding="utf-8-sig")
            print(f"[SUCCESS]- Dữ liệu đã được lưu vào file: {unique_output_file}")

        except Exception as e:
            print(f"[ERROR] Không thể lấy dữ liệu từ bảng: {e}")
            self.send_slack_notification(
                "[FAILED] Chương trình chạy thất bại", self.webhook_url_hddt
            )

    # Chụp màn hình hóa đơn chi tiết
    def capture_full_page(self, driver, save_path):
        try:
            # Reset scroll về đầu trang
            driver.execute_script("""
                var element = document.querySelector('.ant-modal-body');
                if (element) element.scrollTop = 0;
            """)
            time.sleep(1)

            # Đợi modal body xuất hiện
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "ant-modal-body"))
            )
            print("[DEBUG] Đã tìm thấy .ant-modal-body.")

            # Lấy chiều cao tổng và viewport
            total_height = driver.execute_script("""
                var element = document.querySelector('.ant-modal-body');
                return element ? element.scrollHeight : 0;
            """)
            viewport_height = driver.execute_script("""
                var element = document.querySelector('.ant-modal-body');
                return element ? element.clientHeight : 0;
            """)

            print(f"[DEBUG] Chiều cao tổng: {total_height}, Chiều cao viewport: {viewport_height}")

            screenshots = []
            starting_idx = 0
            num_screenshots = total_height // viewport_height
            remainder = total_height % viewport_height
            
            for i in range(1, int(num_screenshots) + 1):
            # while starting_idx < total_height:
                # Scroll đến vị trí hiện tại
                driver.execute_script(f"""
                    var element = document.querySelector('.ant-modal-body');
                    if (element) element.scrollTop = {starting_idx};
                """)
                time.sleep(1.5)

                screenshot_path = f"temp_screenshot_{i}.png"
                driver.save_screenshot(screenshot_path)

                screenshots.append((screenshot_path, viewport_height))

                print(f"[DEBUG] Chụp phần {i}: {starting_idx} -> {starting_idx + viewport_height}")
                
                starting_idx += viewport_height
                
            starting_idx = total_height - viewport_height
            driver.execute_script(f"""
                    var element = document.querySelector('.ant-modal-body');
                    if (element) element.scrollTop = {starting_idx};
                """)
            time.sleep(1.5)
            screenshot_path = f"temp_screenshot_{num_screenshots + 1}.png"
            driver.save_screenshot(screenshot_path)
            screenshots.append((screenshot_path, remainder))
            print(f"[DEBUG] Chụp phần {num_screenshots + 1}: {starting_idx} -> {starting_idx + viewport_height}")

            # Ghép ảnh
            print("[DEBUG] Đang ghép ảnh với tổng chiều cao:", total_height)
            total_width = None
            combined_image = None
            y_offset = 0

            for screenshot_path, top in screenshots:
                img = Image.open(screenshot_path)
                if total_width is None:
                    total_width, _ = img.size
                    combined_image = Image.new("RGB", (total_width, total_height))

                # cropped = img.crop((0, top, total_width, viewport_height))
                cropped = img.crop((0, viewport_height - top, total_width, viewport_height))
                combined_image.paste(cropped, (0, y_offset))
                y_offset += viewport_height
                img.close()

            combined_image.save(save_path)
            print(f"[SUCCESS] Ảnh đã lưu tại: {save_path}")

            # Xóa ảnh tạm
            for screenshot_path, _ in screenshots:
                os.remove(screenshot_path)

        except Exception as e:
            print(f"[ERROR] Lỗi khi chụp màn hình: hóa đơn")



    # 4.1 xuất từng ảnh ( hóa đơn mua vào chi tiết ) của từng hàng dữ liệu bảng
    def extract_img_hoa_don_mua_vao(self, driver):
        try:
            # Tìm tất cả phần tử với class 'ant-table-tbody'
            elements2 = driver.find_elements(By.CLASS_NAME, "ant-table-tbody")
            print(f"[DEBUG] Số phần tử với class='ant-table-tbody': {len(elements2)}")

            # Chọn phần tử thứ hai (index 1)
            if len(elements2) > 1:
                tbody = elements2[1]
            else:
                raise Exception("[DEBUG] Không tìm thấy phần tử ant-table-tbody thứ hai.")

            # Lấy tất cả các hàng hiện tại
            rowsbody = tbody.find_elements(By.XPATH, ".//tr")
            print(f"[DEBUG] Số hàng dữ liệu trong tbody: {len(rowsbody)}")

            # Lặp qua từng hàng và click
            for index, row in enumerate(rowsbody):
                try:
                    # Reset scroll position của trang
                    driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(1)

                    # Đưa con trỏ tới hàng và đảm bảo nó visible
                    driver.execute_script("arguments[0].scrollIntoView(true);", row)
                    ActionChains(driver).move_to_element(row).perform()
                    time.sleep(1)
                    
                    print(f"[DEBUG] Click vào hàng thứ {index + 1}")
                    row.click()
                    time.sleep(2)

                    # Click vào nút "Xem hóa đơn" chi tiết
                    img_btn = driver.find_element(
                        By.XPATH,
                        '//*[@id="__next"]/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[2]/div[1]/div[2]/div/div[5]/button',
                    )
                    img_btn.click()
                    print(f"[SUCCESS]- Finish click btn xem hóa đơn chi tiết ở hàng thứ {index + 1}")
                    time.sleep(3)

                    # Chụp màn hình với viewport mới
                    base_file_name = f"hoadon_muavao_chitiet_stt_{index + 1}.png"
                    unique_file_name = self.get_unique_filename(base_file_name)
                    self.capture_full_page(driver, unique_file_name)

                    # Đóng modal và đợi nó đóng hoàn toàn
                    close_btn = driver.find_element(By.CLASS_NAME, "ant-modal-close")
                    close_btn.click()
                    time.sleep(2)  # Đợi modal đóng hoàn toàn

                except ElementNotInteractableException as e:
                    print(f"[ERROR] Không thể click vào hàng thứ {index + 1}: ")
                except Exception as e:
                    print(f"[ERROR] Lỗi khác xảy ra với hàng thứ {index + 1}: ")

        except Exception as e:
            print(f"[ERROR] Lỗi chung của website khi chụp ảnh hóa đơn failed")
            self.send_slack_notification(
                "[ERROR] Chương trình chạy thất bại", self.webhook_url_hddt
            )
            
    # 4.1 xuất từng ảnh filter ( hóa đơn mua vào chi tiết ) của từng hàng dữ liệu bảng       
    def extract_img_hoa_don_mua_vao_filter(self, driver):
        try:
            # Tìm tất cả phần tử với class 'ant-table-tbody'
            elements2 = driver.find_elements(By.CLASS_NAME, "ant-table-tbody")
            print(f"[DEBUG] Số phần tử với class='ant-table-tbody': {len(elements2)}")

            # Chọn phần tử thứ hai (index 1)
            if len(elements2) > 1:
                tbody = elements2[1]
            else:
                raise Exception("[DEBUG] Không tìm thấy phần tử ant-table-tbody thứ hai.")

            # Lấy tất cả các hàng hiện tại
            rowsbody = tbody.find_elements(By.XPATH, ".//tr")
            print(f"[DEBUG] Số hàng dữ liệu trong tbody: {len(rowsbody)}")

            # Lặp qua từng hàng và click
            for index, row in enumerate(rowsbody):
                try:
                    # Reset scroll position của trang
                    driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(1)

                    # Đưa con trỏ tới hàng và đảm bảo nó visible
                    driver.execute_script("arguments[0].scrollIntoView(true);", row)
                    ActionChains(driver).move_to_element(row).perform()
                    time.sleep(1)
                    
                    print(f"[DEBUG] Click filter vào hàng thứ {index + 1}")
                    row.click()
                    time.sleep(2)

                    # Click vào nút "Xem hóa đơn" chi tiết
                    img_btn = driver.find_element(
                        By.XPATH,
                        '//*[@id="__next"]/section/section/main/div/div/div/div/div[3]/div[2]/div[3]/div[2]/div[1]/div[2]/div/div[5]/button',
                    )
                    img_btn.click()
                    print(f"[SUCCESS]- Finish click btn xem hóa đơn chi tiết ở hàng thứ {index + 1}")
                    time.sleep(3)

                    # Chụp màn hình với viewport mới
                    base_file_name = f"hoadon_muavao_filter_chitiet_stt_{index + 1}.png"
                    unique_file_name = self.get_unique_filename(base_file_name)
                    self.capture_full_page(driver, unique_file_name)

                    # Đóng modal và đợi nó đóng hoàn toàn
                    close_btn = driver.find_element(By.CLASS_NAME, "ant-modal-close")
                    close_btn.click()
                    time.sleep(2)  # Đợi modal đóng hoàn toàn

                except ElementNotInteractableException as e:
                    print(f"[ERROR] Không thể click filter vào hàng thứ {index + 1}: ")
                except Exception as e:
                    print(f"[ERROR] Lỗi khác xảy ra với filter hàng thứ {index + 1}: ")

        except Exception as e:
            print(f"[ERROR] Lỗi chung của website khi chụp ảnh hóa đơn filter failed")
            self.send_slack_notification(
                "[ERROR] Chương trình chạy thất bại", self.webhook_url_hddt
            )
            
            

    # 5. chọn vào tab ( - Tra cứu hóa đơn điện tử bán ra - ) để crawl dữ liệu
    def crawl_hoa_don_ban_ra(self, driver):
        # Chọn Tra cứu hóa đơn điện tử bán ra
        mua_vao_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    '//*[@id="__next"]/section/section/main/div/div/div/div/div[1]/div/div/div/div/div[1]/div[1]/span',
                )
            )
        )
        mua_vao_button.click()
        print("[SUCCESS] - Finish click tab tra cứu hóa đơn bán ra")
        time.sleep(3)

        try:
            # Chờ cho các thẻ input xuất hiện
            inputs = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, '//*[@id="tngay"]/div/input')
                )
            )

            # Chọn thẻ input ở vị trí thứ 1
            target_input_to = inputs[0]

            target_input_to.click()
            print("[SUCCESS] - click thành công vào input")

            # Chỉ truyền số tháng cần lùi
            self.navigate_to_first_day_of_month(
                driver, months_to_go_back=self.args.months_ago
            )
            print("[SUCCESS] - Đã chọn thời gian tìm kiếm.")

        except Exception as e:
            print(f"[ERROR] Gặp lỗi khi thao tác với thẻ input: Tra cứu hóa đơn điện tử bán ra")
            self.send_slack_notification(
                "[ERROR] Chương trình chạy thất bại", self.webhook_url_hddt
            )

        # Chọn nút Tìm kiếm
        tim_kiem = driver.find_element(
            By.XPATH,
            '//*[@id="__next"]/section/section/main/div/div/div/div/div[3]/div[1]/div[3]/div[1]/div/div/form/div[3]/div[1]/button',
        )
        tim_kiem.click()

        print("[SUCCESS] - Finish click tìm kiếm hóa bán ra")
        time.sleep(2)

    # 6. xuất dữ liệu ở trang ( - Tra cứu hóa đơn điện tử bán ra - ) ra file csv
    def extract_table_ban_ra_to_csv(self, driver, output_file_ra):
        """Lấy dữ liệu từ bảng ngang có thanh cuộn và lưu vào file CSV."""

        try:
            # Tạo tên file duy nhất nếu cần
            unique_output_file = self.get_unique_filename(output_file_ra)
            # Chờ bảng hiển thị
            table2 = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        '//*[@id="__next"]/section/section/main/div/div/div/div/div[3]/div[1]/div[3]/div[2]/div[2]/div[3]/div[1]/div[2]/div/div/div/div/div/div[1]/table',
                    )
                )
            )
            # Tìm thanh cuộn ngang
            scrollable_div = driver.find_element(
                By.XPATH,
                "/html/body/div/section/section/main/div/div/div/div/div[3]/div[1]/div[3]/div[2]/div[2]/div[3]/div[1]/div[2]/div/div/div/div/div/div[2]",
            )

            # Lấy chiều rộng cuộn tối đa
            max_scroll_width = driver.execute_script(
                "return arguments[0].scrollWidth;", scrollable_div
            )
            current_scroll_position = 0
            scroll_step = 500  # Số pixel cuộn ngang mỗi lần

            # Khởi tạo lưu trữ dữ liệu
            all_headers = []
            all_rows = []

            while current_scroll_position < max_scroll_width:
                # Lấy HTML hiện tại của bảng có thead class 'ant-table-thead'
                table_html = table2.get_attribute("outerHTML")
                soup = BeautifulSoup(table_html, "html.parser")

                # Lấy tiêu đề nếu tồn tại
                header_row = soup.find("thead")
                if header_row:
                    header_columns = header_row.find_all("th")
                    headers = [header.text.strip() for header in header_columns]
                    # Chỉ thêm các tiêu đề mới
                    if not all_headers:
                        all_headers = headers
                    elif len(headers) > len(all_headers):
                        all_headers += headers[
                            len(all_headers) :
                        ]  # Thêm cột mới vào cuối
                else:
                    print("[DEBUG] Không tìm thấy tiêu đề bảng.")

                # Lấy dữ liệu từ tbody
                # Tìm tất cả phần tử có class 'ant-table-tbody'
                elements2 = driver.find_elements(By.CLASS_NAME, "ant-table-tbody")

                # Chọn phần tử thứ hai (index 1)
                if len(elements2) > 1:
                    tbody = elements2[0]
                else:
                    raise Exception("[DEBUG] Không tìm thấy phần tử ant-table-body thứ hai.")

                # Lấy tất cả các hàng hiện tại
                rowsbody = tbody.find_elements(By.XPATH, ".//tr")
                # Duyệt qua các hàng
                for row in rowsbody:
                    cols = row.find_elements(By.XPATH, "./td")
                    row_data = [col.text.strip() for col in cols]
                    # Đảm bảo chiều dài hàng phù hợp với số cột
                    while len(row_data) < len(all_headers):
                        row_data.append("")  # Thêm ô trống
                    all_rows.append(row_data)

                # Cuộn thanh cuộn ngang
                current_scroll_position += scroll_step
                driver.execute_script(
                    f"arguments[0].scrollLeft = {current_scroll_position};",
                    scrollable_div,
                )
                time.sleep(1)

                # Kiểm tra cuộn xong chưa
                new_scroll_position = driver.execute_script(
                    "return arguments[0].scrollLeft;", scrollable_div
                )
                if new_scroll_position == current_scroll_position:
                    break

            # Lưu vào DataFrame
            if not all_headers:
                print("[ERROR] Không tìm thấy tiêu đề để tạo DataFrame.")
                return

            df = pd.DataFrame(all_rows, columns=all_headers)
            df.to_csv(unique_output_file, index=False, encoding="utf-8-sig")
            print(f"[SUCCESS] - Dữ liệu đã được lưu vào file: {unique_output_file}")
        except Exception as e:
            print(f"[ERROR] Không thể lấy dữ liệu từ bảng: {e}")

    # 6.1 xuất từng ảnh ( hóa đơn bán ra chi tiết ) của từng hàng dữ liệu trong bảng
    def extract_img_hoa_don_ban_ra(self, driver):
        try:
            # Tìm tất cả phần tử với class 'ant-table-tbody'
            elements2 = driver.find_elements(By.CLASS_NAME, "ant-table-tbody")
            print(f"[DEBUG] Số phần tử với class='ant-table-tbody': {len(elements2)}")

            # Chọn phần tử thứ nhất (index 0)
            if len(elements2) > 0:
                tbody = elements2[0]
            else:
                raise Exception("[DEBUG] Không tìm thấy phần tử ant-table-tbody.")

            # Lấy tất cả các hàng hiện tại
            rowsbody = tbody.find_elements(By.XPATH, ".//tr")
            print(f"[DEBUG] Số hàng dữ liệu trong tbody: {len(rowsbody)}")

            # Lặp qua từng hàng và click
            for index, row in enumerate(rowsbody):
                try:
                    # Reset scroll position của trang
                    driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(1)

                    # Đưa con trỏ tới hàng và đảm bảo nó visible
                    driver.execute_script("arguments[0].scrollIntoView(true);", row)
                    ActionChains(driver).move_to_element(row).perform()
                    time.sleep(1)
                    
                    print(f"[DEBUG] Click vào hàng thứ {index + 1}")
                    row.click()
                    time.sleep(2)

                    # Click vào nút "Xem hóa đơn" chi tiết
                    img_btn = driver.find_element(
                        By.XPATH,
                        '//*[@id="__next"]/section/section/main/div/div/div/div/div[3]/div[1]/div[3]/div[2]/div[1]/div[2]/div/div[5]/button',
                    )
                    img_btn.click()
                    print(f"[SUCCESS] - Finish click btn xem hóa đơn chi tiết ở hàng thứ {index + 1}")
                    time.sleep(3)

                    # Chụp màn hình với viewport mới
                    base_file_name = f"hoadon_banra_chitiet_stt_{index + 1}.png"
                    unique_file_name = self.get_unique_filename(base_file_name)
                    self.capture_full_page(driver, unique_file_name)

                    # Đóng modal và đợi nó đóng hoàn toàn
                    close_btn = driver.find_element(By.CLASS_NAME, "ant-modal-close")
                    close_btn.click()
                    time.sleep(2)  # Đợi modal đóng hoàn toàn

                except ElementNotInteractableException as e:
                    print(f"[ERROR] Không thể click vào hàng thứ {index + 1}: ")
                except Exception as e:
                    print(f"[ERROR] Lỗi khác xảy ra với hàng thứ {index + 1}: ")

        except Exception as e:
            print(f"[ERROR] Lỗi chung của website khi chụp ảnh hóa đơn failed")
            self.send_slack_notification(
                "[ERROR] Chương trình chạy thất bại", self.webhook_url_hddt
            )


    # Hàm kết nối PostgreSQL
    def get_connection(self):
        return psycopg2.connect(**self.db_config)

    # Hàm kiểm tra và tạo database nếu chưa tồn tại
    def ensure_database_exists(self, args):
        try:
            # Kết nối đến database tên "postgres" mặc định để kiểm tra kết nối
            connection = psycopg2.connect(
                dbname="postgres",
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
            )
            connection.autocommit = True
            with connection.cursor() as cursor:
                # Kiểm tra nếu database tồn tại
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s", (self.db_name,)
                )
                exists = cursor.fetchone()
                if not exists:
                    # Tạo mới database
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {}").format(
                            sql.Identifier(self.db_name)
                        )
                    )
                    print(f"[SUCCESS] Database '{self.db_name}' created successfully.")
                else:
                    print(f"[DEBUG] Database '{self.db_name}' already exists.")
        except Exception as e:
            print(f"[ERROR] Error ensuring database exists: {e}")
            raise
        finally:
            if connection:
                connection.close()

    def convert_date(self, date_str):
        """Chuyển định dạng ngày từ 'DD/MM/YYYY' sang 'YYYY-MM-DD'."""
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")

    def convert_to_numeric(self, value):
        """Giữ nguyên số có dấu '.' và nếu trống thì lưu rỗng."""
        if isinstance(value, str) and value.strip():
            return value
        return ""

    def get_latest_file(self, pattern):
        files = list(Path(".").glob(pattern))
        if not files:
            print(f"No files found matching pattern: {pattern}")
            return None
        latest_file = max(files, key=os.path.getmtime)
        return str(latest_file)

    

    def get_latest_files_by_timestamp(self, csv_pattern, img_pattern):
        """
        Trả về danh sách các file CSV mới nhất phù hợp với pattern và các ảnh liên quan.
        Mỗi cặp (csv_file, list_of_images) tương ứng với 1 file CSV.
        """
        try:
            # Get all matching files with their timestamps
            csv_files = list(Path(".").glob(csv_pattern))
            img_files = list(Path(".").glob(img_pattern))

            if not csv_files:
                print(f"[DEBUG] Không tìm thấy file CSV với pattern: {csv_pattern}")
                return None, []

            # Sắp xếp theo thời gian tạo mới nhất trước
            csv_files.sort(key=os.path.getctime, reverse=True)
            result = []
            
            # Get the latest CSV file
            latest_csv = max(csv_files, key=os.path.getctime)
            print(f"[DEBUG] Latest CSV file: {latest_csv}")

            # Get timestamp of the latest CSV file
            csv_timestamp = os.path.getctime(latest_csv)

            # Filter images created after the CSV file
            relevant_images = [
                img for img in img_files if os.path.getctime(img) >= csv_timestamp
            ]

            # Sort images by creation time
            relevant_images.sort(key=os.path.getctime)

            # Print the relevant images found
            if relevant_images:
                print("[DEBUG] Relevant images found:")
                for img in relevant_images:
                    print(f"- {img}")
            else:
                print("[DEBUG] No relevant images found for the latest CSV.")

            return str(latest_csv), [str(img) for img in relevant_images]

        except Exception as e:
            print(f"[ERROR] Error getting latest files: ")
            return None, []

    # ==================== Cấp quyền cho tệp trên Google Drive ==================== #
    def set_permissions(self, service, file_id):
        """Cấp quyền truy cập cho tệp trên Google Drive."""
        permission = {
            "type": "anyone",
            "role": "writer",  # Cấp quyền 'writer' cho phép xem, sửa và xóa
        }
        try:
            service.permissions().create(fileId=file_id, body=permission).execute()
            print(f"[SUCCESS] Permissions set for file ID: {file_id}")
        except Exception as error:
            print(
                f"[ERROR] An error occurred while setting permissions for {file_id}: {error}"
            )

    # ==================== Tạo thư mục hóa đơn trên Google Drive ==================== #
    def create_invoice_directory_on_drive(self, service, company_id, company_name):
        """Tạo thư mục hóa đơn trên Google Drive và trả về ID của thư mục chính và thư mục con."""
        # Tìm thư mục chính 'HoaDon'
        query = "mimeType='application/vnd.google-apps.folder' and name='HoaDon'"
        response = service.files().list(q=query, fields="files(id)").execute()

        if not response.get("files"):
            # Nếu thư mục chính chưa tồn tại, tạo mới
            folder_metadata = {
                "name": "HoaDon",
                "mimeType": "application/vnd.google-apps.folder",
            }
            folder = service.files().create(body=folder_metadata, fields="id").execute()
            main_folder_id = folder.get("id")
            print(f"[SUCCESS] Folder created: HoaDon with ID: {main_folder_id}")
        else:
            main_folder_id = response.get("files")[0].get("id")
            print(f"[INFO] Folder already exists: HoaDon with ID: {main_folder_id}")

        # Tạo thư mục con với tên công ty và thời gian hiện tại
        current_time = datetime.now()
        subfolder_name = f"{company_id}_{company_name}_{current_time.strftime('%d/%m/%Y_%H:%M:%S')}"
        subfolder_metadata = {
            "name": subfolder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [main_folder_id],
        }

        subfolder = (
            service.files().create(body=subfolder_metadata, fields="id").execute()
        )
        subfolder_id = subfolder.get("id")
        print(f"[SUCCESS] Subfolder created: {subfolder_name} with ID: {subfolder_id}")

        # Cấp quyền truy cập cho thư mục chính và thư mục con
        self.set_permissions(service, main_folder_id)
        self.set_permissions(service, subfolder_id)

        # Thông báo đường dẫn tới thư mục chính và thư mục con
        print(
            f"[INFO] Link to main folder: https://drive.google.com/drive/folders/{main_folder_id}"
        )
        print(
            f"[INFO] Link to subfolder: https://drive.google.com/drive/folders/{subfolder_id}"
        )
        self.send_slack_notification(
            f"[INFO] Link to subfolder: https://drive.google.com/drive/folders/{subfolder_id}",
            self.webhook_url_hddt,
        )

        return main_folder_id, subfolder_id

    # ==================== Tải ảnh lên Google Drive ==================== #
    def upload_image_to_drive(self, service, file_path, folder_id):
        """Tải ảnh lên Google Drive và trả về đường dẫn tải xuống."""
        if not os.path.exists(file_path):
            print(f"[ERROR] File not found: {file_path}")
            return None

        file_metadata = {"name": os.path.basename(file_path), "parents": [folder_id]}
        media = MediaFileUpload(file_path, mimetype="image/png")

        try:
            # Tải ảnh lên Google Drive
            file = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            file_id = file.get("id")

            # Cấp quyền công khai cho tệp
            self.set_permissions(service, file_id)

            # Tạo và trả về URL tải xuống
            image_url = f"https://drive.google.com/uc?id={file_id}"
            print(f"[SUCCESS] File uploaded to Drive: {image_url}")
            return image_url
        except HttpError as error:
            print(f"[ERROR] Failed to upload file to Drive: {error}")
            return None
        except Exception as e:
            print(f"[ERROR] Unexpected error during upload: {e}")
            return None

    
    # Hàm lưu dữ liệu vào database
    def save_to_database(self, data, image_paths, drive_image_paths, company_id, company_name, loai_hoa_don):
        success_count = 0
        failed_count = 0

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for idx, (_, row) in enumerate(data.iterrows()):
                    try:
                        drive_image_path = (
                            drive_image_paths[idx] if idx < len(drive_image_paths) else ""
                        )

                        so_hoa_don = str(row.get("so_hoa_don", ""))

                        # Chuyển đổi ngày lập
                        try:
                            ngay_lap = self.convert_date(row.get("ngay_lap", "01/01/1970"))
                        except Exception as date_error:
                            print(f"[WARNING] Lỗi convert ngày ở dòng {idx + 1}: {date_error}")
                            ngay_lap = "1970-01-01"  # fallback an toàn

                        invoice_values = (
                            row.get("mau_so", ""),
                            row.get("ky_hieu", ""),
                            so_hoa_don,
                            ngay_lap,
                            row.get("mst_nguoi_mua", ""),
                            row.get("ten_nguoi_mua", ""),
                            row.get("mst_nguoi_ban", ""),
                            row.get("ten_nguoi_ban", ""),
                            self.convert_to_numeric(row.get("tong_tien_chua_thue", "")),
                            self.convert_to_numeric(row.get("tong_tien_thue", "")),
                            self.convert_to_numeric(row.get("tong_tien_chiet_khau", "")),
                            self.convert_to_numeric(row.get("tong_tien_phi", "")),
                            self.convert_to_numeric(row.get("tong_tien_thanh_toan", "")),
                            row.get("don_vi_tien_te", ""),
                            row.get("trang_thai", ""),
                            drive_image_path,
                            company_id,
                            company_name,
                            loai_hoa_don
                        )

                        invoice_values = [v if pd.notna(v) else "" for v in invoice_values]

                        invoice_query = """
                            INSERT INTO data_hoadon (mau_so, ky_hieu, so_hoa_don, ngay_lap, mst_nguoi_mua, ten_nguoi_mua, mst_nguoi_ban, ten_nguoi_ban, 
                                                    tong_tien_chua_thue, tong_tien_thue, tong_tien_chiet_khau, tong_tien_phi, 
                                                    tong_tien_thanh_toan, don_vi_tien_te, trang_thai, image_drive_path, created_at, company_id, company_name, loai_hoa_don)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s)
                            ON CONFLICT (company_id, so_hoa_don, loai_hoa_don) DO UPDATE
                            SET mau_so = EXCLUDED.mau_so,
                                ngay_lap = EXCLUDED.ngay_lap,
                                mst_nguoi_mua = EXCLUDED.mst_nguoi_mua,
                                ten_nguoi_mua = EXCLUDED.ten_nguoi_mua,
                                mst_nguoi_ban = EXCLUDED.mst_nguoi_ban,
                                ten_nguoi_ban = EXCLUDED.ten_nguoi_ban,
                                tong_tien_chua_thue = EXCLUDED.tong_tien_chua_thue,
                                tong_tien_thue = EXCLUDED.tong_tien_thue,
                                tong_tien_chiet_khau = EXCLUDED.tong_tien_chiet_khau,
                                tong_tien_phi = EXCLUDED.tong_tien_phi,
                                tong_tien_thanh_toan = EXCLUDED.tong_tien_thanh_toan,
                                don_vi_tien_te = EXCLUDED.don_vi_tien_te,
                                trang_thai = EXCLUDED.trang_thai,
                                image_drive_path = EXCLUDED.image_drive_path,
                                created_at = CURRENT_TIMESTAMP;
                        """

                        cur.execute(invoice_query, invoice_values)
                        success_count += 1

                    except Exception as e:
                        print(f"[ERROR] ❌ Dòng {idx + 1} bị lỗi khi insert DB: {e}")
                        failed_count += 1
                        continue

        print(f"[SUMMARY] ✅ Đã lưu thành công {success_count} dòng, ❌ thất bại {failed_count} dòng.")


    # Quy trình database chính
    def main_db_workflow(self, service, company_id, company_name, username, password):
        # Tạo bảng nếu chưa tồn tại
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(self.CREATE_TABLE_QUERY)
                cur.execute(self.ADD_FOREIGN_KEY_QUERY)
                
        # Tạo thư mục trên Google Drive
        main_folder_id, subfolder_id = self.create_invoice_directory_on_drive(
            service, company_id, company_name
        )
        
        for loai_hoa_don, csv_pattern, img_pattern in [
            ("mua vào có mã", "hoa_don_mua_vao_co*.csv", "hoadon_muavao_chitiet_stt_*.png"),
            ("mua vào không mã", "hoa_don_mua_vao_filter*.csv", "hoadon_muavao_filter_chitiet_stt_*.png"),
            ("bán ra", "hoa_don_ban_ra*.csv", "hoadon_banra_chitiet_stt_*.png"),
        ]:
            csv_file, images = self.get_latest_files_by_timestamp(csv_pattern, img_pattern)
            if csv_file:
                print(f"[DEBUG] Processing {loai_hoa_don} data from {csv_file}")
                df1 = pd.read_csv(csv_file)
                
                
                # Đổi tên cột cho phù hợp với schema database
                if loai_hoa_don == "mua vào có mã":
                    column_mapping = {
                        "Ký hiệumẫu số": "mau_so",
                        "Ký hiệuhóa đơn": "ky_hieu",
                        "Số hóa đơn": "so_hoa_don",
                        "Ngày lập": "ngay_lap",
                        "Thông tin người bán": "thong_tin_hoa_don_mua_vao_co_ma",  # CHUẨN
                        "Tổng tiềnchưa thuế": "tong_tien_chua_thue",
                        "Tổng tiền thuế": "tong_tien_thue",
                        "Tổng tiềnchiết khấuthương mại": "tong_tien_chiet_khau",
                        "Tổng tiền phí": "tong_tien_phi",
                        "Tổng tiềnthanh toán": "tong_tien_thanh_toan",
                        "Đơn vịtiền tệ": "don_vi_tien_te",
                        "Trạng tháihóa đơn": "trang_thai",
                    }
                    df1.rename(columns=column_mapping, inplace=True)
                    print("[DEBUG] Đã đổi tên các cột trong file CSV:")
                    # print(df1.head())

                    # Xử lý dữ liệu trong file CSV
                    df2 = df1.copy()
                elif loai_hoa_don == "mua vào không mã":
                    column_mapping = {
                        "Ký hiệumẫu số": "mau_so",
                        "Ký hiệuhóa đơn": "ky_hieu",
                        "Số hóa đơn": "so_hoa_don",
                        "Ngày lập": "ngay_lap",
                        "Thông tin người bán": "thong_tin_hoa_don_mua_vao_khong_ma",
                        "Tổng tiềnchưa thuế": "tong_tien_chua_thue",
                        "Tổng tiền thuế": "tong_tien_thue",
                        "Tổng tiềnchiết khấuthương mại": "tong_tien_chiet_khau",
                        "Tổng tiền phí": "tong_tien_phi",
                        "Tổng tiềnthanh toán": "tong_tien_thanh_toan",
                        "Đơn vịtiền tệ": "don_vi_tien_te",
                        "Trạng tháihóa đơn": "trang_thai",
                    }
                    df1.rename(columns=column_mapping, inplace=True)
                    print("[DEBUG] Đã đổi tên các cột trong file CSV:")
                    # print(df1.head())

                    # Xử lý dữ liệu trong file CSV
                    df2 = df1.copy()
                elif loai_hoa_don == "bán ra":
                    column_mapping = {
                        "Ký hiệumẫu số": "mau_so",
                        "Ký hiệuhóa đơn": "ky_hieu",
                        "Số hóa đơn": "so_hoa_don",
                        "Ngày lập": "ngay_lap",
                        "Thông tin hóa đơn": "thong_tin_hoa_don_ban_ra",
                        "Tổng tiềnchưa thuế": "tong_tien_chua_thue",
                        "Tổng tiền thuế": "tong_tien_thue",
                        "Tổng tiềnchiết khấuthương mại": "tong_tien_chiet_khau",
                        "Tổng tiền phí": "tong_tien_phi",
                        "Tổng tiềnthanh toán": "tong_tien_thanh_toan",
                        "Đơn vịtiền tệ": "don_vi_tien_te",
                        "Trạng tháihóa đơn": "trang_thai",
                    }
                    df1.rename(columns=column_mapping, inplace=True)
                    print("[DEBUG] Đã đổi tên các cột trong file CSV:")
                    # print(df1.head())

                    # Xử lý dữ liệu trong file CSV
                    df2 = df1.copy()

                    
                    

                
                
                # df1.rename(columns=column_mapping, inplace=True)
                # print("[DEBUG] Đã đổi tên các cột trong file CSV:")
                # # print(df1.head())

                # # Xử lý dữ liệu trong file CSV
                # df2 = df1.copy()
                if loai_hoa_don == "bán ra":
                    # Tách MST người mua và Tên người mua từ cột "Thông tin hóa đơn"
                    df2[['mst_nguoi_mua', 'ten_nguoi_mua']] = df2['thong_tin_hoa_don_ban_ra'].str.extract(r'MST người mua:\s*([\d\-]+)\s*\n\s*Tên người mua:\s*(.+)')
                    # Chèn các cột mới vào vị trí tương ứng
                    thong_tin_index = df2.columns.get_loc('thong_tin_hoa_don_ban_ra')
                    df2.insert(thong_tin_index, 'mst_nguoi_mua', df2.pop('mst_nguoi_mua'))
                    df2.insert(thong_tin_index + 1, 'ten_nguoi_mua', df2.pop('ten_nguoi_mua'))
                    print("[DEBUG] Đã xử lý dữ liệu cho hóa đơn bán ra:")
                    # print(df2[['thong_tin_hoa_don_ban_ra', 'mst_nguoi_mua', 'ten_nguoi_mua']].head())
                elif loai_hoa_don == "mua vào có mã":
                    # Tách MST người bán và Tên người bán từ cột "Thông tin người bán"
                    df2[['mst_nguoi_ban', 'ten_nguoi_ban']] = df2['thong_tin_hoa_don_mua_vao_co_ma'].str.extract(r'MST người bán:\s*([\d\-]+)\s*\n\s*Tên người bán:\s*(.+)')
                    # Chèn các cột mới vào vị trí tương ứng
                    thong_tin_index = df2.columns.get_loc('thong_tin_hoa_don_mua_vao_co_ma')
                    df2.insert(thong_tin_index, 'mst_nguoi_ban', df2.pop('mst_nguoi_ban'))
                    df2.insert(thong_tin_index + 1, 'ten_nguoi_ban', df2.pop('ten_nguoi_ban'))
                    print("[DEBUG] Đã xử lý dữ liệu cho hóa đơn mua vào có mã:")
                    # print(df2[['thong_tin_hoa_don_mua_vao_co_ma', 'mst_nguoi_ban', 'ten_nguoi_ban']].head())
                elif loai_hoa_don == "mua vào không mã":
                    # Tách MST người bán và Tên người bán từ cột "Thông tin người bán"
                    df2[['mst_nguoi_ban', 'ten_nguoi_ban']] = df2['thong_tin_hoa_don_mua_vao_khong_ma'].str.extract(r'MST người bán:\s*([\d\-]+)\s*\n\s*Tên người bán:\s*(.+)')
                    # Chèn các cột mới vào vị trí tương ứng
                    thong_tin_index = df2.columns.get_loc('thong_tin_hoa_don_mua_vao_khong_ma')
                    df2.insert(thong_tin_index, 'mst_nguoi_ban', df2.pop('mst_nguoi_ban'))
                    df2.insert(thong_tin_index + 1, 'ten_nguoi_ban', df2.pop('ten_nguoi_ban'))
                    print("[DEBUG] Đã xử lý dữ liệu cho hóa đơn mua vào có mã:")
                    # print(df2[['thong_tin_hoa_don_mua_vao_khong_ma', 'mst_nguoi_ban', 'ten_nguoi_ban']].head())
                
                
                

                # Lưu dữ liệu vào Driver va database
                drive_image_paths = [
                    self.upload_image_to_drive(service, img, subfolder_id) for img in images if os.path.exists(img)
                ]

                self.save_to_database(df2, images, drive_image_paths, company_id, company_name, loai_hoa_don)
                print(f"[DEBUG] Processed {loai_hoa_don} data from {csv_file}")
                print(f"[DEBUG] Rows in df2: {df2.shape[0]}, Images: {len(images)}, Drive Images: {len(drive_image_paths)}")
                
                
    # Hàm lấy dữ liệu từ bảng company_information
    def fetch_company_information(self):
        query = (
            "SELECT company_id, company_name, hoadon_username, hoadon_password FROM company_information;"
        )
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(query)
                    rows = cur.fetchall()

                    # Lọc các công ty không có username hoặc password
                    filtered_rows = [
                        row
                        for row in rows
                        if row["hoadon_username"] and row["hoadon_password"]
                    ]

                    return filtered_rows
        except Exception as e:
            print(f"[ERROR] Error fetching data from 'company_information': {e}")
            return []

    def clean_data(self, directory_path=".", file_extensions=(".csv", ".png")):
        """
        Xóa tất cả các file dữ liệu trong thư mục được chỉ định có phần mở rộng cụ thể.

        Args:
            directory_path (str): Đường dẫn đến thư mục chứa file dữ liệu. Mặc định là thư mục hiện tại.
            file_extensions (tuple): Các phần mở rộng của file cần xóa. Mặc định là ('.csv', '.pdf').

        Returns:
            None
        """
        try:
            files_removed = 0
            for file_name in os.listdir(directory_path):
                if file_name.endswith(file_extensions):
                    file_path = os.path.join(directory_path, file_name)
                    os.remove(file_path)
                    files_removed += 1
                    print(f"[INFO] Đã xóa file: {file_path}")

            if files_removed == 0:
                print(
                    f"[INFO] Không có file nào với đuôi {file_extensions} trong thư mục '{directory_path}' để xóa."
                )
            else:
                print(f"[INFO] Tổng số file đã xóa: {files_removed}")

        except Exception as e:
            print(f"[ERROR] Lỗi khi xóa dữ liệu: {e}")

    # Hàm main
    def main_logic(self):
        """Chạy chương trình chính"""
        print("[INFO] Main logic: Workflow HoaDonDienTu.")
        args = self.parse_arguments()
        self.ensure_database_exists(args)

        self.db_config = {
            "dbname": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
            "host": self.db_host,
            "port": self.db_port,
        }
        driver = self.initialize_driver()
        self.send_slack_notification("======== Workflow HoaDonDienTu ==========", self.webhook_url_hddt)

        service = self.initialize_drive_service()
        if not service:
            print("[ERROR] Google Drive service not initialized. Exiting.")
            exit(1)

        output_file = "hoa_don_mua_vao_co.csv"
        output_file_filter = "hoa_don_mua_vao_filter.csv"
        output_file_ra = "hoa_don_ban_ra.csv"
        captcha_image_path = "captcha_image.svg"

        total_companies = 0
        company_results = {}

        try:
            company_data_list = self.fetch_company_information()
            if not company_data_list:
                print("Không có công ty nào để xử lý. Kết thúc chương trình.")
                driver.quit()
                return
            elif args.company and args.company != "None":
                company_data_list = [company_data for company_data in company_data_list if company_data["company_name"] == args.company]
                if not company_data_list:
                    print(f"[DEBUG] Không có công ty nào với tên '{args.company}'. Kết thúc chương trình.")
                    self.driver.quit()
                    return

            total_companies = len(company_data_list)
            print(f"Tổng số công ty cần xử lý: {total_companies}")

            # Lấy ngày đầu tháng hiện tại
            current_month = datetime.now().replace(day=1)
            
            # Bắt đầu từ tháng trước theo tham số --months-ago
            start_month = current_month - relativedelta(months=args.months_ago + 1)
            
            # Tạo danh sách các tháng cần crawl
            months_to_crawl = [
                (start_month - relativedelta(months=i)).strftime("%m/%Y")
                for i in range(args.crawl_months)
            ]


            for idx, company_data in enumerate(company_data_list, start=1):
                company_id, company_name, username, password = (
                    company_data["company_id"],
                    company_data["company_name"],
                    company_data["hoadon_username"],
                    company_data["hoadon_password"],
                )
                print(f"Đang xử lý công ty thứ {idx}: {company_id} - {company_name}")

                success_months = []
                failed_months = []

                driver.execute_script("window.open('');")
                new_tab = driver.window_handles[-1]
                driver.switch_to.window(new_tab)

                try:
                    self.login_to_hoadondientu(driver, username, password, company_id, company_name)
                    self.crawl_img(driver)
                    self.enter_verification_code(driver, captcha_image_path)
                    # self.submit_form(driver, captcha_image_path)
                    
                    try:
                        self.submit_form(driver, captcha_image_path)
                    except Exception as e:
                        continue  # Bỏ qua công ty này và thử với công ty tiếp theo

                    for i, month in enumerate(months_to_crawl, start=1):
                        print(f"[DEBUG] Đang cào tháng {month} ({i}/{args.crawl_months})")
                        try:
                            # self.navigate_to_first_day_of_month(driver, month)
                            self.crawl_hoa_don_mua_vao(driver)
                            self.extract_table_mua_vao_to_csv(driver, output_file)
                            self.extract_img_hoa_don_mua_vao(driver)
                            
                            self.crawl_hoa_don_mua_vao_filter(driver)
                            self.extract_table_mua_vao_to_csv_filter(driver, output_file_filter)
                            self.extract_img_hoa_don_mua_vao_filter(driver)
                            
                            self.crawl_hoa_don_ban_ra(driver)
                            self.extract_table_ban_ra_to_csv(driver, output_file_ra)
                            self.extract_img_hoa_don_ban_ra(driver)
                            self.main_db_workflow(service, company_id, company_name, username, password)
                            success_months.append(month)
                        except Exception as e:
                            print(
                                f"[ERROR] Thất bại khi xử lý tháng {month} cho công ty {company_id} - {company_name}: "
                            )
                            failed_months.append(month)
                            continue

                    company_results[company_id] = (success_months, failed_months)

                except Exception as e:
                    print(f"[ERROR] Lỗi khi xử lý công ty {company_id} - {company_name}: ")
                    company_results[company_id] = ([], months_to_crawl)
                finally:
                    driver.close()
                    if driver.window_handles:
                        driver.switch_to.window(driver.window_handles[0])

        except Exception as e:
            print(f"[ERROR] An error occurred: {e}")
        finally:
            if not any(success for success, fail in company_results.values()):
                company_results = {
                    company_data["company_id"]: ([], months_to_crawl)
                    for company_data in company_data_list
                }
            
            print("\n=========== Báo cáo tổng kết ===========")
            print(f"Số công ty cần chạy: {total_companies}")
            
            print("Danh sách tháng thực sự lấy dữ liệu:", months_to_crawl)

            print(
                f"Số tháng cần crawl: {args.crawl_months} ({', '.join(months_to_crawl)})"
            )
            print(
                f"Số công ty thành công: {sum(1 for success, fail in company_results.values() if success)}"
            )
            print(
                f"Số công ty thất bại: {sum(1 for success, fail in company_results.values() if not success)}"
            )

            self.send_slack_notification(
                "\n=========== Báo cáo tổng kết ===========", self.webhook_url_hddt
            )
            self.send_slack_notification(
                f"Số công ty cần chạy: {total_companies}", self.webhook_url_hddt
            )
            self.send_slack_notification(
                f"Số tháng cần crawl: {args.crawl_months} ({', '.join(months_to_crawl)})",
                self.webhook_url_hddt,
            )
            self.send_slack_notification(
                f"Số công ty thành công: {sum(1 for success, fail in company_results.values() if success)}",
                self.webhook_url_hddt,
            )
            self.send_slack_notification(
                f"Số công ty thất bại: {sum(1 for success, fail in company_results.values() if not success)}",
                self.webhook_url_hddt,
            )

            for company_id, (success_months, failed_months) in company_results.items():
                success_text = f"Thành công {len(success_months)} tháng" + (
                    f" ({', '.join(success_months)})" if success_months else ""
                )
                fail_text = f"Thất bại {len(failed_months)} tháng" + (
                    f" ({', '.join(failed_months)})" if failed_months else ""
                )
                print(f"Công ty với id {company_id}: {success_text}, {fail_text}")
                self.send_slack_notification(
                    f"Công ty với id {company_id}: {success_text}, {fail_text}", self.webhook_url_hddt
                )
            self.clean_data(".", file_extensions=(".csv", ".png"))
            driver.quit()
            print("Driver closed.")
            # New