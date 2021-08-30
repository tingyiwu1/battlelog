import brawlstats
from sqlalchemy import Column, Integer, String, ForeignKey, Table, Boolean, DateTime, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session
from declaritive import Base, battle_comp, Battle, Map, Comp, Brawler, Player
from client import client
from data import *

# logging.basicConfig(filename='db.log')
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

engine = create_engine('sqlite:///battlelog.db')
# engine = create_engine('sqlite:///test3.db')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session: Session = DBSession()

if __name__ == '__main__':
    battles = []
    for tag in searchtags:
        try:
            battles += client.get_battle_logs(tag).raw_data
        except (brawlstats.errors.UnexpectedError, brawlstats.errors.NotFoundError) as e:
            print('Error on ' + tag)
            print(e)
            continue
    # with open('battlelog2.json', 'r') as f:
    #     battles = json.load(f)

    add_battles(battles)
    # battles = [b.map for b in (
    #     session.query(Battle)
    #     # .filter(Battle.type == None)
    #     .order_by(Battle.battle_time)
    #     .all()
    # )]
    # print(len(battles))
    print(session.query(func.count(Battle.battle_id)).one()[0])
    
    # player = (
    #     session.query(Player)
    #     .filter(Player.player_tag == 'P29RYYQCP')
    #     .one_or_none()
    # )
    # player.analyze()
    # player.get_info(client)
    # battles = (
    #             session.query(Player)
    #             .filter(Player.name == 'frown :(')
    #             .one_or_none()
    #         ).battles
    # for b in battles:
    #     print(b)

    # objs = session.query(Comp).join(Battle).join(Map).filter(Map.api_id == '15000010')
    # key = lambda c: len(c.battles)
    # objs = sorted(objs, key=key, reverse=True)

    # for i in range(25):
    #     print(objs[i], key(objs[i]))
        
    # with open('battlelog2.json', 'w') as f:
    #     f.write(export_battles())

