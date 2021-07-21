from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from declaritive import Base, Battle, Map, Comp, Brawler, Player, engine
from datetime import datetime
import json

searchtags = ['9YCR98LV', '29CUPUL8', 'YYY0VRUY', '9YVUV8PR', 'GUPR2P8U', 'PRUQQGUL']
# searchtags = ['9YCR98LV']

Base.metadata.create_all(engine)
Base.metadata.bind = engine


DBSession = sessionmaker(bind=engine)
session = DBSession()

def get_players(sort=Player.player_tag):
    return session.query(Player).order_by(sort).all()

def export_battles():
    battles = (
        session.query(Battle)
        .order_by(Battle.battle_time.desc())
        .all()
    )
    battlelist = []
    for battle in battles:
        if sp := battle.raw_data.get('starPlayer'):
            if sp['tag'] in searchtags:
                battle.raw_data['result'] = 'victory'
            else:
                battle.raw_data['result'] = 'defeat'
        else:
            battle.raw_data['result'] = None

        battlelist.append({
        'battleTime': battle.battle_time.strftime('%Y%m%dT%H%M%S') + '.000Z',
        'event': {
            'id': battle.map.api_id,
            'mode': battle.map.mode,
            'map': battle.map.name
        },
        'battle': battle.raw_data
    })
    # battlelist = [{
    #     'battleTime': battle.battle_time.strftime('%Y%m%dT%H%M%S') + '.000Z',
    #     'event': {
    #         'id': battle.map.api_id,
    #         'mode': battle.map.mode,
    #         'map': battle.map.name
    #     },
    #     'battle': battle.raw_data
    # } for battle in battles]
    return json.dumps(battlelist, ensure_ascii=True, indent=2)

def add_player(player_tag, name):
    player = (
        session.query(Player)
        .filter(Player.player_tag == player_tag)
        .one_or_none()
    )

    if player:
        if player.name != name:
            player.name = name
            session.commit()
        return player

    player = Player(player_tag=player_tag, name=name)
    
    print(player)
    session.add(player)
    session.commit()
    return player

def add_players(players):
    # (player_tag, name)
    existing_players = session.query(Player).all()
    new_players = []
    for player in players:
        for p in existing_players:
            if player[0] == p.player_tag:
                if player[1] != p.name:
                    p.name = player[1]
                player = p
                break
        
        if isinstance(player, Player):
            new_players.append(player)
            continue
        
        player = Player(player_tag = player[0], name = player[1])
        print(player)
        session.add(player)
        new_players.append(player)

    session.commit()
    return new_players

def add_brawler(id, name):
    brawler = (
        session.query(Brawler)
        .filter(Brawler.brawler_id == id)
        .one_or_none()
    )

    if brawler:
        if brawler.name != name:
            brawler = name
            session.commit()
        return brawler
    
    brawler = Brawler(brawler_id=id, name=name)
    session.add(brawler)
    session.commit()
    return brawler

def add_battle(time, map_name, mode, map_api_id, battle_json):
    raw_data = {k:v for k,v in battle_json.items() if k not in ['trophyChange']}

    battle = (
        session.query(Battle)
        .join(Map)
        .filter(
            and_(
                Battle.battle_time == time,
                Map.name == map_name,
                Map.mode == mode,
                Map.api_id == map_api_id,
            )
        )
        .one_or_none()
    )
    if battle:
        if battle.raw_data != raw_data:
            battle.raw_data = raw_data
            session.commit()
        return battle

    map = add_map(map_name, mode, map_api_id)

    battle = Battle(battle_time = time, type = raw_data.get('type') , raw_data = raw_data)
    battle.map = map
    players = (raw_data.get('players') or [player for team in raw_data.get('teams') for player in team]) + ([raw_data['bigBrawler']] if raw_data.get('bigBrawler') else [])
    ps = add_players([(p['tag'].lstrip('#'), p['name']) for p in players])
    battle.players += ps
    for player in players:
        # p = add_player(player['tag'].lstrip('#'), player['name'])
        # battle.players.append(p)
        # p.battles.append(battle)
        b = add_brawler(player['brawler']['id'], player['brawler']['name'])
        battle.brawlers.append(b)

    if teams := raw_data.get('teams'):
        for team in teams:
            c = add_comp([player['brawler']['id'] for player in team])
            battle.comps.append(c)

    print(battle)
    session.add(battle)
    session.commit()
    return battle

def add_battles(battles_json):
    battles_json = sorted(battles_json, key=lambda b: Battle.timestamp_as_datetime(b['battleTime']))

    epoch = Battle.timestamp_as_datetime(battles_json[0]['battleTime'])

    recent = (
        session.query(Battle.battle_time, Map.api_id, Battle.raw_data)
        .join(Map)
        .order_by(Battle.battle_time)
        .all()
    )

    new_battles = []
    new_comp_ids = set()
    new_map_modes = set()
    new_map_api_ids = set()
    new_brawler_ids = set()
    new_player_tags = set()
    

    for battle in battles_json:
        battle_time = Battle.timestamp_as_datetime(battle['battleTime'])
        map_name = battle['event']['map']
        mode = battle['event'].get('mode') or battle['battle']['mode']
        map_api_id = battle['event']['id']
        raw_data = {k:v for k,v in battle['battle'].items() if k not in ['trophyChange']}

        for b in recent:
            if b[:-1] == (battle_time, map_api_id):
                # if b.raw_data != raw_data:
                #     b.raw_data = raw_data
                battle = b
                break
        
        if not isinstance(battle, dict):
            recent.append(battle)
            continue
        
        new_map_modes.add(mode)
        new_map_api_ids.add(map_api_id)

        map_kwargs = {
            'name': map_name,
            'mode': mode,
            'api_id': map_api_id
        }

        players = (raw_data.get('players') or [player for team in raw_data.get('teams') for player in team]) + ([raw_data['bigBrawler']] if raw_data.get('bigBrawler') else [])
        
        player_kwargs = []
        brawler_kwargs = []
        for player in players:
            player_tag = player['tag'].lstrip('#')
            player_kwargs.append({
                'player_tag': player_tag,
                'name': player['name']
            })
            new_player_tags.add(player_tag)

            brawler_id = player['brawler']['id']
            brawler_kwargs.append({
                'brawler_id': brawler_id,
                'name': player['brawler']['name']
            })
            new_brawler_ids.add(brawler_id)

        comp_kwargs = []
        if teams := raw_data.get('teams'):
            for team in teams:
                comp_id = Comp.hash_comp([player['brawler']['id'] for player in team])
                comp_kwargs.append({
                    'comp_id': comp_id
                })
                new_comp_ids.add(comp_id)

        new_battles.append(
            ({
                'battle_time': battle_time,
                'type': raw_data.get('type'),
                'raw_data': raw_data
            }, map_kwargs, player_kwargs, brawler_kwargs, comp_kwargs)
        )
        recent.append((battle_time, map_api_id, raw_data))
    
    existing_players = (
        session.query(Player)
        .filter(Player.player_tag.in_(new_player_tags))
        .all()
    )
    existing_comps = (
        session.query(Comp)
        .filter(Comp.comp_id.in_(new_comp_ids))
        .all()
    )
    existing_maps = (
        session.query(Map)
        .filter(
            and_(
                Map.mode.in_(new_map_modes),
                Map.api_id.in_(new_map_api_ids)
            )
        )
        .all()
    )
    existing_brawlers = (
        session.query(Brawler)
        .filter(Brawler.brawler_id.in_(new_brawler_ids))
        .all()
    )

    processed_battles = []
    
    for battle in new_battles:
        battle_obj = Battle(**battle[0])

        map = None
        for m in existing_maps:
            if m.mode == battle[1]['mode'] and m.api_id == battle[1]['api_id']:
                if m.name != battle[1]['name']:
                    m.name = battle[1]['name']
                map = m
                break
        if not isinstance(map, Map):
            map = Map(**battle[1])
            session.add(map)
            print(map)
            existing_maps.append(map)
        battle_obj.map = map

        for player in battle[2]:
            for p in existing_players:
                if p.player_tag == player['player_tag']:
                    if p.name != player['name']:
                        p.name = player['name']
                    player = p
                    break
            if not isinstance(player, Player):
                player = Player(**player)
                session.add(player)
                print(player)
                existing_players.append(player)
            battle_obj.players.append(player)
        
        for brawler in battle[3]:
            for b in existing_brawlers:
                if b.brawler_id == brawler['brawler_id']:
                    if b.name != brawler['name']:
                        b.name = brawler['name']
                    brawler = b
                    break
            if not isinstance(brawler, Brawler):
                brawler = Brawler(**brawler)
                session.add(brawler)
                print(brawler)
                existing_brawlers.append(brawler)
            battle_obj.brawlers.append(brawler)

        for comp in battle[4]:
            for c in existing_comps:
                if c.comp_id == comp['comp_id']:
                    comp = c
                    break
            if not isinstance(comp, Comp):
                comp = Comp(**comp)
                brawler_ids = Comp.unhash_comp(comp.comp_id)
                for b in existing_brawlers:
                    if b.brawler_id in brawler_ids:
                        comp.brawlers.append(b)
                        brawler_ids.remove(b.brawler_id)
                    if not len(brawler_ids):
                        break
                session.add(comp)
                print(comp)
                existing_comps.append(comp)
            battle_obj.comps.append(comp if isinstance(comp, Comp) else Comp(**comp))

        processed_battles.append(battle_obj)
        session.add(battle_obj)
        print(battle_obj)

    session.commit()
    return processed_battles


def add_map(name, mode, api_id):
    map = (
        session.query(Map)
        .filter(
            and_(
                Map.mode == mode,
                Map.api_id == api_id
            )
        )
        .one_or_none()
    )

    if map:
        if map.name != name:
            map.name = name
            session.commit()
        return map

    map = Map(name = name, mode = mode, api_id = api_id)

    print(map)
    session.add(map)
    session.commit()
    return map

def add_comp(brawler_ids):

    def get_brawlers_from_ids(ids):
        # return session.query(Brawler).filter(or_(*[Brawler.brawler_id == id for id in ids])).all()
        return [session.query(Brawler).filter(Brawler.brawler_id == id).one_or_none() for id in ids]

    id = Comp.hash_comp(brawler_ids)
    comp = (
        session.query(Comp)
        .filter(Comp.comp_id == id)
        .one_or_none()
    )
    if comp:
        if comp.comp_id != (h := Comp.hash_comp([brawler.brawler_id for brawler in comp.brawlers])):
            comp.brawlers = get_brawlers_from_ids(Comp.unhash_comp(h))
        return comp
    
    comp = Comp(comp_id = id)
    comp.brawlers = get_brawlers_from_ids(brawler_ids)

    print(comp)
    session.add(comp)
    session.commit()
    return comp

if __name__ == '__main__':
    # brawlers = client.get_brawlers()
    # for brawler in brawlers:
    #     add_brawler(id=brawler['id'], name=brawler['name'])

    # battles = client.get_battle_logs('9YCR98LV').raw_data
    with open('battlelog.json', 'r') as f:
        battles = json.load(f)
    for battle in reversed(battles):
        add_battle(
            time = datetime.strptime(battle['battleTime'][:-5], '%Y%m%dT%H%M%S'), 
            map_name = battle['event']['map'],
            mode = battle['event'].get('mode') or battle['battle']['mode'],
            map_api_id = battle['event']['id'],
            battle_json = battle['battle']
        )
    with open('battlelog.json', 'w') as f:
        f.write(export_battles())
    # print(
    #     [n.type for n in session.query(Player)
    #     .filter(
    #         Player.player_tag == '9YCR98LV'
    #     )
    #     .one_or_none()
    #     .battles]
    # )
    bs = (
        session.query(Battle)
        .order_by(Battle.battle_time.desc())
        .limit(20)
        .all()
    )
    for b in bs:
        print(b)