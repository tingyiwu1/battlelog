from sqlalchemy import Column, Integer, String, ForeignKey, Table, JSON, Unicode, DateTime, create_engine
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

BRAWLER_ID_BASE = 16000000

Base = declarative_base()

engine = create_engine('sqlite:///battlelog.db')
# engine = create_engine('sqlite:///test3.db')

battle_brawler = Table(
    'battle_brawler',
    Base.metadata,
    Column('battle_id', Integer, ForeignKey('battle.battle_id')),
    Column('brawler_id', Integer, ForeignKey('brawler.brawler_id')),
)

battle_comp = Table(
    'battle_comp',
    Base.metadata,
    Column('battle_id', Integer, ForeignKey('battle.battle_id')),
    Column('comp_id', Integer, ForeignKey('comp.comp_id')),
)

brawler_comp = Table(
    'brawler_comp',
    Base.metadata,
    Column('brawler_id', Integer, ForeignKey('brawler.brawler_id')),
    Column('comp_id', Integer, ForeignKey('comp.comp_id'))
)

player_battle = Table(
    'player_battle',
    Base.metadata,
    Column('player_id', Integer, ForeignKey('player.player_id')),
    Column('battle_id', Integer, ForeignKey('battle.battle_id'))
)

class Player(Base):
    __tablename__ = 'player'
    player_id = Column(Integer, primary_key=True)
    player_tag = Column(String)
    name = Column(Unicode)
    battles = relationship(
        'Battle', secondary=player_battle, back_populates='players'
    )
    def analyze(self):
        battle_count = len(self.battles)
        wins = 0
        losses = 0
        non_comp_games = 0
        for b in self.battles:
            result = b.raw_data.get("rank") or b.raw_data.get("result")
            if result == 'victory':
                wins += 1
            elif result == 'defeat':
                losses += 1
            if len(b.raw_data.get('teams') or []) != 2:
                non_comp_games += 1
        print(f'{self.name} #{self.player_tag}')
        print(f'total battles: {battle_count}')
        print(f'wins: {wins}')
        print(f'losses: {losses}')
        print(f'non 3v3 games: {non_comp_games}')

    def get_info(self, client):
        data = client.get_player('#' + self.player_tag).raw_data
        print(f'{self.name} #{self.player_tag}')
        print(f'trophies: {data.get("trophies")}')
        print(f'highest trophies: {data.get("highestTrophies")}')
        print(f'highest power play points: {data.get("highestPowerPlayPoints")}')
        print(f'club: {data.get("club").get("name")}')

    def __repr__(self):
        return f'<{self.name} #{self.player_tag}>'

class Battle(Base):
    __tablename__ = 'battle'
    battle_id = Column(Integer, primary_key=True)
    map_id = Column(Integer, ForeignKey('map.map_id'))
    map = relationship(
        'Map', back_populates='battles'
    )
    type = Column(String)
    battle_time = Column(DateTime)
    raw_data = Column(JSON)
    brawlers = relationship(
        'Brawler', secondary=battle_brawler, back_populates='battles'
    )
    players = relationship(
        'Player', secondary=player_battle, back_populates='battles'
    )
    comps = relationship(
        'Comp', secondary=battle_comp, back_populates='battles'
    )
    @staticmethod
    def timestamp_as_datetime(timestamp):
        return datetime.strptime(timestamp[:-5], '%Y%m%dT%H%M%S')

    def __repr__(self):
        return f'<{self.raw_data.get("rank") or self.raw_data.get("result")} | {repr(self.map)} | {" ".join([repr(p) for p in self.players])}>'

class Map(Base):
    __tablename__ = 'map'
    map_id = Column(Integer, primary_key=True)
    api_id = Column(Integer)
    name = Column(String)
    mode = Column(String)
    battles = relationship(
        'Battle', back_populates='map'
    )
    def __repr__(self):
        return f'{self.name} ({self.mode})'

class Comp(Base):
    __tablename__ = 'comp'
    comp_id = Column(Integer, primary_key=True)
    brawlers = relationship(
        'Brawler', secondary=brawler_comp, back_populates='comps'
    )
    battles = relationship(
        'Battle', secondary=battle_comp, back_populates='comps'
    )
    
    @staticmethod
    def hash_comp(ids):
        # 1{brawler_id}{brawler_id}{brawler_id}
        return int('1' + ''.join([str(i)[-3:] for i in sorted(ids, key=lambda k: int(k))]))
    @staticmethod
    def unhash_comp(h):
        stripped = str(h).lstrip('1')
        return [BRAWLER_ID_BASE + int(stripped[i:i+3]) for i in range(0, len(stripped), 3)]
    def __repr__(self):
        return f'<{"|".join([repr({b.brawler_id:b for b in self.brawlers}[k]) for k in Comp.unhash_comp(self.comp_id)])}>'

class Brawler(Base):
    __tablename__ = 'brawler'
    brawler_id = Column(Integer, primary_key=True)
    name = Column(String)
    battles = relationship(
        'Battle', secondary=battle_brawler, back_populates='brawlers'
    )
    comps = relationship(
        'Comp', secondary=brawler_comp, back_populates='brawlers'
    )
    def __repr__(self):
        return self.name

Base.metadata.create_all(engine)