# src/data/universe_loader.py

"""
ETF Universe
load and manage predefined ETF universe
"""

import yaml
import yfinance as yf
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path
import logging
import pickle
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

@dataclass
class ETF:
    """basic ETF info"""
    ticker: str
    category: str
    asset_class: str

    # load from API
    name: str = ""
    expense_ratio: float = ""
    description: str = ""
    ytd_return: float = 0.0
    
    last_updated: datetime = None

    def __repr__(self):
        return f"ETF({self.ticker}: {self.name})"
    
    def __hash__(self):
        return hash(self.ticker)

    def is_stale(self, max_age_days: int = 30) -> bool:
        """check if data are stale"""
        if not self.last_updated:
            return True
        age = datetime.now() - self.last_updated
        return age > timedelta(days=max_age_days)

# @dataclass
# class TLHPair:
#     """Tax Loss Harvesting pairing"""
#     primary: str
#     substitute: str
#     correlation: float
#     note: str = ""

@dataclass
class PortfolioTemplate:
    """Portfolio template"""
    name: str
    description: str
    allocation: Dict[str, float]
    
    def validate(self) -> bool:
        """validate if it makes sense"""
        total = sum(self.allocation.values())
        return abs(total - 1.0) < 0.01  # allow 1% deviation


class ETFInfoFetcher:
    """
    get ETF data from yfinance
    """
    
    @staticmethod
    def fetch_etf_info(ticker: str, retry_count: int = 3) -> Dict:
        """
        get ETF info
        
        Returns:
        --------
        Dict with keys: name, expense_ratio, description, total_assets, ytd_return
        """
        for attempt in range(retry_count):
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                result = {
                    'name': info.get('longName') or info.get('shortName') or ticker,
                    'expense_ratio': info.get('annualReportExpenseRatio', 0.0) or 0.0,
                    'description': info.get('longBusinessSummary', ''),
                    'total_assets': info.get('totalAssets', 0.0) or 0.0,
                    'ytd_return': info.get('ytdReturn', 0.0) or 0.0,
                }
                
                logger.debug(f"Fetched info for {ticker}: {result['name']}")
                return result
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {ticker}: {e}")
                if attempt < retry_count - 1:
                    time.sleep(1)  # retry after one second
                else:
                    logger.error(f"Failed to fetch info for {ticker} after {retry_count} attempts")
                    # return default
                    return {
                        'name': ticker,
                        'expense_ratio': 0.0,
                        'description': '',
                        'total_assets': 0.0,
                        'ytd_return': 0.0,
                    }
    
    @staticmethod
    def batch_fetch_etf_info(tickers: List[str], 
                             delay: float = 0.5) -> Dict[str, Dict]:
        """
        get ETF info 
        
        Parameters:
        -----------
        delay : float
            delay between requests
        """
        results = {}
        total = len(tickers)
        
        logger.info(f"Fetching info for {total} ETFs...")
        
        for i, ticker in enumerate(tickers, 1):
            logger.info(f"Fetching {i}/{total}: {ticker}")
            results[ticker] = ETFInfoFetcher.fetch_etf_info(ticker)
            
            # avoid rate limiting
            if i < total:
                time.sleep(delay)
        
        return results


class ETFUniverse:
    """
    ETF Universe manager
    
    Functions：
    - Load ETFs
    - Select ETFs
    - TLH pairs
    - portfolio templates
    """

    def __init__(self, 
                 config_path: str = "",
                 cache_path: str = ""):
        BASE_DIR = Path(__file__).resolve().parents[2]
        print(BASE_DIR)
        self.config_path = Path(BASE_DIR / "config"/"etf_universe.yaml")
        self.cache_path = Path(BASE_DIR / "data" / "cache"/"etf_info_cache.pkl")
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.etfs: Dict[str, ETF] = {}
        self.tlh_pairs: Dict[str, str] = {}
        self.templates: Dict[str, PortfolioTemplate] = {}
        
        self._by_asset_class: Dict[str, List[ETF]] = {}
        self._by_category: Dict[str, List[ETF]] = {}
        
        self._category_names: Dict[str, str] = {}
        self._asset_class_mapping: Dict[str, List[str]] = {}
        
        self.load_universe()
    
    def load_universe(self, refresh_info: bool = False):
        """
        load ETF universe
        Params:
            refresh_info: force refresh API data
        """ 

        try:
            with open(self.config_path) as f:
                config = yaml.safe_load(f)
            
            logger.info(f"Loading ETF universe from {self.config_path}")
            
            # load ETFs
            self._category_names = config.get('categories', {})
            self._asset_class_mapping = config.get('asset_classes', {})
            
            ticker_structure = config['etf_universe']

            cached_info = self._load_cache() if not refresh_info else {}
            

            all_tickers = []
            ticker_metadata = {}  # {ticker: (asset_class, subcategory)}
            for asset_class, subcategories in ticker_structure.items():
                for ticker_dict in subcategories:
                    # print(ticker_dict)
                    ticker = ticker_dict['ticker']
                    all_tickers.append(ticker)
                    ticker_metadata[ticker] = (asset_class, ticker_dict['category'])
            
            # identify tickers to get data from API
            tickers_to_fetch = []
            for ticker in all_tickers:
                if ticker not in cached_info or cached_info[ticker].is_stale():
                    tickers_to_fetch.append(ticker)
            
            # batch fetch data
            if tickers_to_fetch:
                logger.info(f"Fetching fresh data for {len(tickers_to_fetch)} ETFs")
                fresh_info = ETFInfoFetcher.batch_fetch_etf_info(tickers_to_fetch)
                
                # refresh cache
                for ticker, info in fresh_info.items():
                    asset_class, subcategory = ticker_metadata[ticker]
                    category_name = self._category_names.get(subcategory, subcategory)
                    
                    cached_info[ticker] = ETF(
                        ticker=ticker,
                        category=category_name,
                        asset_class=asset_class,
                        name=info['name'],
                        expense_ratio=info['expense_ratio'],
                        description=info['description'],
                        # total_assets=info['total_assets'],
                        ytd_return=info['ytd_return'],
                        last_updated=datetime.now()
                    )
            
            # build ETF dictionary
            for ticker, etf in cached_info.items():
                if ticker in all_tickers:  # keep tickers in config
                    self.etfs[ticker] = etf
                    
                    # build dictionary
                    if etf.asset_class not in self._by_asset_class:
                        self._by_asset_class[etf.asset_class] = []
                    self._by_asset_class[etf.asset_class].append(etf)
                    
                    if etf.category not in self._by_category:
                        self._by_category[etf.category] = []
                    self._by_category[etf.category].append(etf)
            
            # update cache
            self._save_cache(cached_info)

            logger.info(
                f"Loaded {len(self.etfs)} ETFs, "
                f"{len(self.tlh_pairs)} TLH pairs, "
                f"{len(self.templates)} templates"
            )
            
        except Exception as e:
            logger.error(f"Error loading ETF universe: {e}")
            raise
    
    def _load_cache(self) -> Dict[str, ETF]:
        """load data in cache"""
        if not self.cache_path.exists():
            return {}
        
        try:
            with open(self.cache_path, 'rb') as f:
                cached = pickle.load(f)
            logger.info(f"Loaded {len(cached)} ETFs from cache")
            return cached
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
            return {}
    
    def _save_cache(self, etf_info: Dict[str, ETF]):
        """save ETF data to cache"""
        try:
            with open(self.cache_path, 'wb') as f:
                pickle.dump(etf_info, f)
            logger.debug(f"Saved {len(etf_info)} ETFs to cache")
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

   
    def _load_tlh_pairs(self, pairs_config: List[Dict]):
        """Tax Loss Harvesting"""
        for pair_data in pairs_config:
            primary = pair_data['primary']
            substitute = pair_data['substitute']
            
            self.tlh_pairs[primary] = substitute
            
            logger.debug(f"TLH pair: {primary} → {substitute}")
    
    # def _load_templates(self, templates_config: Dict):
    #     """portfolio templates"""
    #     for key, template_data in templates_config.items():
    #         template = PortfolioTemplate(
    #             name=template_data['name'],
    #             description=template_data['description'],
    #             allocation=template_data['allocation']
    #         )
            
    #         if not template.validate():
    #             logger.warning(
    #                 f"Template {key} allocation doesn't sum to 1.0"
    #             )
            
    #         self.templates[key] = template
    
    # ========== getters ==========
    
    def get_etf(self, ticker: str) -> Optional[ETF]:
        """get single ETF info"""
        return self.etfs.get(ticker.upper())
    
    def get_all_tickers(self) -> List[str]:
        """get list of all tickers"""
        return sorted(self.etfs.keys())
    
    def get_by_asset_class(self, asset_class: str) -> List[ETF]:
        """get list by asset class"""
        return self._by_asset_class.get(asset_class, [])
    
    def get_by_category(self, category: str) -> List[ETF]:
        """get list by category"""
        return self._by_category.get(category, [])
    
    def get_equity_etfs(self) -> List[ETF]:
        """get all equity ETFs"""
        return self.get_by_asset_class('equity')
    
    def get_bond_etfs(self) -> List[ETF]:
        """get all bond ETFs"""
        return self.get_by_asset_class('fixed_income')
    
    def get_alternative_etfs(self) -> List[ETF]:
        """get all alternative ETFs"""
        return self.get_by_asset_class('alternatives')
    
    def search_etfs(self, 
                    keyword: str = None,
                    asset_classes: List[str] = None,
                    max_expense_ratio: float = None) -> List[ETF]:
        """
        Search ETFs
        
        Parameters:
        -----------
        keyword : str
            search in name or description
        asset_classes : List[str]
            filter by asset class
        max_expense_ratio : float
            max expense ratio
        """
        results = list(self.etfs.values())
        
        # by keyword
        if keyword:
            keyword = keyword.lower()
            results = [
                etf for etf in results
                if keyword in etf.name.lower() 
                or keyword in etf.category.lower()
            ]
        
        # by asset class
        if asset_classes:
            results = [
                etf for etf in results
                if etf.asset_class in asset_classes
            ]
        
        # search by expense ratio
        if max_expense_ratio is not None:
            results = [
                etf for etf in results
                if etf.expense_ratio <= max_expense_ratio
            ]
        
        return results

    def refresh_etf_info(self, ticker: str):
        """refresh etf info"""
        logger.info(f"Refreshing info for {ticker}")
        info = ETFInfoFetcher.fetch_etf_info(ticker)
        
        if ticker in self.etfs:
            etf = self.etfs[ticker]
            etf.name = info['name']
            etf.expense_ratio = info['expense_ratio']
            etf.description = info['description']
            # etf.total_assets = info['total_assets']
            etf.ytd_return = info['ytd_return']
            etf.last_updated = datetime.now()
            
            # update cache
            cached = self._load_cache()
            cached[ticker] = etf
            self._save_cache(cached)
    
    def refresh_all(self):
        """refresh all ETF"""
        logger.info("Refreshing all ETF information")
        self.load_universe(refresh_info=True)

    
    # ========== TLH ==========
    
    # def get_tlh_substitute(self, ticker: str) -> Optional[str]:
    #     """get tax loss harvesting substitute"""
    #     return self.tlh_pairs.get(ticker.upper())
    
    # def has_tlh_pair(self, ticker: str) -> bool:
    #     """check if tlh pair exists"""
    #     return ticker.upper() in self.tlh_pairs
    
    # ========== Template ==========
    
    # def get_template(self, name: str) -> Optional[PortfolioTemplate]:
    #     """get portfolio template"""
    #     return self.templates.get(name)
    
    # def get_all_templates(self) -> Dict[str, PortfolioTemplate]:
    #     """get all templates"""
    #     return self.templates
    
    # ========== utils ==========
    
    def validate_tickers(self, tickers: List[str]) -> Tuple[List[str], List[str]]:
        """
        validate ticker lists
        
        Returns:
        --------
        (valid_tickers, invalid_tickers)
        """
        valid = []
        invalid = []
        
        for ticker in tickers:
            if ticker.upper() in self.etfs:
                valid.append(ticker.upper())
            else:
                invalid.append(ticker)
        
        return valid, invalid
    
    def get_etf_summary(self, ticker: str) -> str:
        """ETF summary"""
        etf = self.get_etf(ticker)
        if not etf:
            return f"ETF {ticker} not found"
        
        return (
            f"{etf.ticker}: {etf.name}\n"
            f"Category: {etf.category}\n"
            f"Asset Class: {etf.asset_class}\n"
            f"Expense Ratio: {etf.expense_ratio*100:.2f}%\n"
            f"Total Assets: ${etf.total_assets/1e9:.2f}B\n"
            f"YTD Return: {etf.ytd_return*100:.2f}%"
        )
    
    def __repr__(self):
        return (
            f"ETFUniverse("
            f"etfs={len(self.etfs)}, "
        )


# ========== other ==========

def load_default_universe(refresh: bool = False) -> ETFUniverse:
    """
    load default ETF universe
    
    Parameters:
    -----------
    refresh : bool
        refresh API data
    """
    universe = ETFUniverse()
    if refresh:
        universe.refresh_all()
    return universe



if __name__ == "__main__":
    # test codes
    logging.basicConfig(level=logging.INFO)
    universe = load_default_universe()
    

