import asyncio

from app.domain.strategies.strategy_service import StrategyService
from app.core.universe import load_universe
from app.db.session import async_session


async def main():
    print("🚀 Script started")

    # Load universe
    symbols = load_universe()
    print("📊 Loaded symbols:", symbols)

    if not symbols:
        print("❌ No symbols found. Check universe.yaml path/config")
        return

    async with async_session() as db:
        print("✅ DB session created")

        service = StrategyService(db)

        print("⚙️ Running strategy...")
        results = await service.process_universe(symbols[:5])

        print("\n🎯 FINAL RESULTS:")
        for r in results:
            print(r)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("❌ ERROR OCCURRED:", str(e))
