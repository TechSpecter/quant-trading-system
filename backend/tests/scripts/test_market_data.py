import asyncio

from app.domain.market.market_data_service import MarketDataService
from app.core.universe import load_universe
from app.db.session import async_session


async def main():
    symbols = load_universe()

    async with async_session() as db:
        service = MarketDataService(db)

        data = await service.get_universe_data(symbols[:3], timeframe="D")

        for k, v in data.items():
            if v is not None:
                print(f"\n{k}")
                print(v.head())
            else:
                print(f"{k} -> No Data")


if __name__ == "__main__":
    asyncio.run(main())
