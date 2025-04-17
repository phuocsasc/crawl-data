import sys
import os

# Add the src directory to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from src.crawler_baohiemxahoi import crawler_baohiemxahoi
from src.crawler_thuedientu import crawler_thuedientu
from src.crawler_hoadondientu import crawler_hoaddondientu

if __name__ == '__main__':
    crawler_bhxh = crawler_baohiemxahoi()
    crawler_bhxh.main_logic()
    # crawler_tdt = crawler_thuedientu()
    # crawler_tdt.main_logic()
    # crawler_hddt = crawler_hoaddondientu()
    # crawler_hddt.main_logic()