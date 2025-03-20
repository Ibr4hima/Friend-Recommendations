# mapper.py
import sys

def map_friends():
    """
    Mapper function that processes the input file and emits key-value pairs.
    For each user and their friends, emits:
    1. Direct friendships (user, friend) -> 'direct'
    2. Potential friendships (friend1, friend2) -> user (mutual friend)
    """
    for line in sys.stdin:
        # Skip empty lines
        if not line.strip():
            continue

        # Parse input line
        parts = line.strip().split('\t')
        if len(parts) != 2:
            continue

        user = parts[0]
        friends = parts[1].split(',')

        # Emit direct friendships
        for friend in friends:
            if friend:
                # Emit both (user, friend) and (friend, user) since friendships are mutual
                key = '\t'.join(sorted([user, friend]))
                print(f"{key}\tdirect")

        # Emit potential friendships (mutual friends)
        for i in range(len(friends)):
            for j in range(i + 1, len(friends)):
                friend1 = friends[i]
                friend2 = friends[j]
                if friend1 and friend2:
                    key = '\t'.join(sorted([friend1, friend2]))
                    print(f"{key}\t{user}")

if __name__ == "__main__":
    map_friends()