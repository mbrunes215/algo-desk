"""
Position Manager

Tracks all positions across multiple platforms (IBKR, Kalshi).
Stores position data in SQLite and provides real-time P&L tracking.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime
from enum import Enum

try:
    from sqlalchemy import (
        create_engine,
        Column,
        String,
        Float,
        Integer,
        DateTime,
        Enum as SQLEnum,
    )
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker, Session
except ImportError:
    raise ImportError(
        "sqlalchemy is required: pip install sqlalchemy"
    )


logger = logging.getLogger(__name__)

Base = declarative_base()


class PositionStatus(Enum):
    """Position status enumeration."""

    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class PositionModel(Base):
    """SQLAlchemy model for position storage."""

    __tablename__ = "positions"

    id = Column(String(50), primary_key=True)
    platform = Column(String(20), nullable=False)  # 'IBKR' or 'KALSHI'
    symbol = Column(String(20), nullable=False)
    side = Column(String(10), nullable=False)  # 'BUY' or 'SELL'
    quantity = Column(Float, nullable=False)
    entry_price = Column(Float, nullable=False)
    current_price = Column(Float, nullable=False)
    pnl = Column(Float, default=0.0)
    pnl_percent = Column(Float, default=0.0)
    status = Column(SQLEnum(PositionStatus), default=PositionStatus.OPEN)
    opened_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)

    def __repr__(self):
        return (
            f"<PositionModel(id={self.id}, platform={self.platform}, "
            f"symbol={self.symbol}, qty={self.quantity}, pnl={self.pnl})>"
        )


@dataclass
class Position:
    """Position data structure."""

    position_id: str
    platform: str
    symbol: str
    side: str
    quantity: float
    entry_price: float
    current_price: float
    pnl: float
    pnl_percent: float
    status: PositionStatus
    opened_at: datetime
    updated_at: datetime
    closed_at: Optional[datetime] = None

    @property
    def market_value(self) -> float:
        """Calculate market value of position."""
        return self.quantity * self.current_price

    @property
    def cost_basis(self) -> float:
        """Calculate cost basis."""
        return self.quantity * self.entry_price

    @property
    def exposure_pct(self) -> float:
        """Calculate exposure as percentage of market value."""
        return self.pnl_percent


class PositionManager:
    """
    Manages all positions across multiple trading platforms.

    Stores positions in SQLite database and provides P&L tracking,
    position queries, and exposure calculations.
    """

    def __init__(self, db_path: str = "positions.db"):
        """
        Initialize position manager.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        logger.info(f"PositionManager initialized with database: {db_path}")

    def add_position(
        self,
        position_id: str,
        platform: str,
        symbol: str,
        side: str,
        quantity: float,
        entry_price: float,
        current_price: float,
    ) -> Optional[Position]:
        """
        Add or update a position.

        Args:
            position_id: Unique position ID
            platform: 'IBKR' or 'KALSHI'
            symbol: Asset symbol
            side: 'BUY' or 'SELL'
            quantity: Position size
            entry_price: Entry price
            current_price: Current market price

        Returns:
            Position object or None if failed
        """
        session = self.Session()

        try:
            # Check if position already exists
            existing = (
                session.query(PositionModel)
                .filter_by(id=position_id)
                .first()
            )

            # Calculate P&L
            if side.upper() == "BUY":
                pnl = (current_price - entry_price) * quantity
            else:  # SELL
                pnl = (entry_price - current_price) * quantity

            pnl_percent = (pnl / (entry_price * quantity)) * 100 if entry_price else 0

            if existing:
                # Update existing position
                existing.current_price = current_price
                existing.pnl = pnl
                existing.pnl_percent = pnl_percent
                existing.updated_at = datetime.utcnow()
                logger.debug(f"Updated position: {position_id}")
            else:
                # Create new position
                new_position = PositionModel(
                    id=position_id,
                    platform=platform,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    entry_price=entry_price,
                    current_price=current_price,
                    pnl=pnl,
                    pnl_percent=pnl_percent,
                    status=PositionStatus.OPEN,
                )
                session.add(new_position)
                logger.info(
                    f"Added position: {position_id} | {platform} {symbol} "
                    f"{side} x{quantity}"
                )

            session.commit()

            # Fetch and return the position
            pos = (
                session.query(PositionModel)
                .filter_by(id=position_id)
                .first()
            )
            return self._model_to_position(pos) if pos else None

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to add position: {e}")
            return None
        finally:
            session.close()

    def close_position(
        self,
        position_id: str,
        close_price: float,
    ) -> bool:
        """
        Close a position.

        Args:
            position_id: Position ID to close
            close_price: Close price

        Returns:
            True if successful, False otherwise
        """
        session = self.Session()

        try:
            position = (
                session.query(PositionModel)
                .filter_by(id=position_id)
                .first()
            )

            if not position:
                logger.warning(f"Position not found: {position_id}")
                return False

            # Calculate final P&L
            if position.side.upper() == "BUY":
                pnl = (close_price - position.entry_price) * position.quantity
            else:
                pnl = (position.entry_price - close_price) * position.quantity

            position.pnl = pnl
            position.current_price = close_price
            position.status = PositionStatus.CLOSED
            position.closed_at = datetime.utcnow()
            position.updated_at = datetime.utcnow()

            session.commit()
            logger.info(
                f"Closed position: {position_id} | {position.symbol} "
                f"pnl=${pnl:.2f}"
            )
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to close position: {e}")
            return False
        finally:
            session.close()

    def get_position_by_id(self, position_id: str) -> Optional[Position]:
        """
        Get a position by ID.

        Args:
            position_id: Position ID

        Returns:
            Position or None if not found
        """
        session = self.Session()

        try:
            pos = (
                session.query(PositionModel)
                .filter_by(id=position_id)
                .first()
            )
            return self._model_to_position(pos) if pos else None

        except Exception as e:
            logger.error(f"Failed to get position: {e}")
            return None
        finally:
            session.close()

    def get_positions_by_symbol(self, symbol: str) -> List[Position]:
        """
        Get all positions for a symbol.

        Args:
            symbol: Asset symbol

        Returns:
            List of Position objects
        """
        session = self.Session()

        try:
            positions = (
                session.query(PositionModel)
                .filter_by(symbol=symbol)
                .all()
            )
            return [self._model_to_position(p) for p in positions]

        except Exception as e:
            logger.error(f"Failed to get positions for {symbol}: {e}")
            return []
        finally:
            session.close()

    def get_positions_by_platform(self, platform: str) -> List[Position]:
        """
        Get all positions on a specific platform.

        Args:
            platform: 'IBKR' or 'KALSHI'

        Returns:
            List of Position objects
        """
        session = self.Session()

        try:
            positions = (
                session.query(PositionModel)
                .filter_by(platform=platform)
                .all()
            )
            return [self._model_to_position(p) for p in positions]

        except Exception as e:
            logger.error(
                f"Failed to get positions for platform {platform}: {e}"
            )
            return []
        finally:
            session.close()

    def get_all_open_positions(self) -> List[Position]:
        """
        Get all open positions.

        Returns:
            List of Position objects
        """
        session = self.Session()

        try:
            positions = (
                session.query(PositionModel)
                .filter_by(status=PositionStatus.OPEN)
                .all()
            )
            return [self._model_to_position(p) for p in positions]

        except Exception as e:
            logger.error(f"Failed to get open positions: {e}")
            return []
        finally:
            session.close()

    def get_total_exposure(self) -> Dict[str, float]:
        """
        Calculate total exposure across all positions.

        Returns:
            Dict with keys: gross_notional, net_notional, long_notional,
            short_notional
        """
        session = self.Session()

        try:
            positions = (
                session.query(PositionModel)
                .filter_by(status=PositionStatus.OPEN)
                .all()
            )

            gross_notional = 0.0
            net_notional = 0.0
            long_notional = 0.0
            short_notional = 0.0

            for pos in positions:
                notional = pos.quantity * pos.current_price
                gross_notional += abs(notional)

                if pos.side.upper() == "BUY":
                    net_notional += notional
                    long_notional += notional
                else:
                    net_notional -= notional
                    short_notional -= notional

            return {
                "gross_notional": gross_notional,
                "net_notional": abs(net_notional),
                "long_notional": long_notional,
                "short_notional": abs(short_notional),
            }

        except Exception as e:
            logger.error(f"Failed to calculate exposure: {e}")
            return {}
        finally:
            session.close()

    def get_pnl(self) -> Dict[str, float]:
        """
        Calculate total P&L across all positions.

        Returns:
            Dict with keys: realized, unrealized, total
        """
        session = self.Session()

        try:
            open_positions = (
                session.query(PositionModel)
                .filter_by(status=PositionStatus.OPEN)
                .all()
            )
            closed_positions = (
                session.query(PositionModel)
                .filter_by(status=PositionStatus.CLOSED)
                .all()
            )

            unrealized_pnl = sum(pos.pnl for pos in open_positions)
            realized_pnl = sum(pos.pnl for pos in closed_positions)
            total_pnl = unrealized_pnl + realized_pnl

            return {
                "realized": realized_pnl,
                "unrealized": unrealized_pnl,
                "total": total_pnl,
            }

        except Exception as e:
            logger.error(f"Failed to calculate P&L: {e}")
            return {"realized": 0.0, "unrealized": 0.0, "total": 0.0}
        finally:
            session.close()

    def update_market_prices(self, prices: Dict[str, float]) -> None:
        """
        Update market prices for all positions.

        Args:
            prices: Dict mapping symbol to current price
        """
        session = self.Session()

        try:
            for symbol, price in prices.items():
                positions = (
                    session.query(PositionModel)
                    .filter_by(symbol=symbol)
                    .all()
                )

                for pos in positions:
                    # Recalculate P&L
                    if pos.side.upper() == "BUY":
                        pnl = (price - pos.entry_price) * pos.quantity
                    else:
                        pnl = (pos.entry_price - price) * pos.quantity

                    pnl_percent = (
                        (pnl / (pos.entry_price * pos.quantity)) * 100
                        if pos.entry_price
                        else 0
                    )

                    pos.current_price = price
                    pos.pnl = pnl
                    pos.pnl_percent = pnl_percent
                    pos.updated_at = datetime.utcnow()

            session.commit()
            logger.debug(f"Updated prices for {len(prices)} symbols")

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to update prices: {e}")
        finally:
            session.close()

    def _model_to_position(self, model: PositionModel) -> Position:
        """Convert PositionModel to Position dataclass."""
        return Position(
            position_id=model.id,
            platform=model.platform,
            symbol=model.symbol,
            side=model.side,
            quantity=model.quantity,
            entry_price=model.entry_price,
            current_price=model.current_price,
            pnl=model.pnl,
            pnl_percent=model.pnl_percent,
            status=model.status,
            opened_at=model.opened_at,
            updated_at=model.updated_at,
            closed_at=model.closed_at,
        )
