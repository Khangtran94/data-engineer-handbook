df1 = medal_match_player.merge(match_detail, on = ['match_id','player_gamertag'])
df = df1.merge(medal, on = 'medal_id')\
    .merge(matches,on = 'match_id')\
        .merge(map, on = 'mapid')

Which player averages the most kills per game?
Which playlist gets played the most?
Which map gets played the most?
Which map do players get the most Killing Spree medals on?
