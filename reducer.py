# reducer.py
import sys
from collections import defaultdict

def parse_mapper_output(line):
    """Parse a line from mapper output into key and value."""
    parts = line.strip().split('\t')
    if len(parts) < 2:
        return None, None
    key = tuple(parts[:2])
    value = parts[2] if len(parts) > 2 else None
    return key, value

def reduce_recommendations():
    """
    Reducer function that processes mapper output and generates recommendations.
    For each pair of users, it:
    1. Checks if they are direct friends.
    2. Counts the number of mutual friends if they are not direct friends.
    """
    current_pair = None
    values = []

    user_recommendations = defaultdict(lambda: defaultdict(int))

    for line in sys.stdin:
        # Parse input
        key, value = parse_mapper_output(line)
        if not key:
            continue

        if current_pair != key:
            if current_pair and 'direct' not in values:
                # Not direct friends, so accumulate mutual friends
                user_a, user_b = current_pair
                mutual_friends = set(values)
                count = len(mutual_friends)

                # Update recommendations for both users
                user_recommendations[user_a][user_b] += count
                user_recommendations[user_b][user_a] += count

            # Reset for new key
            current_pair = key
            values = []

        values.append(value)

    # Process last pair
    if current_pair and 'direct' not in values:
        user_a, user_b = current_pair
        mutual_friends = set(values)
        count = len(mutual_friends)
        user_recommendations[user_a][user_b] += count
        user_recommendations[user_b][user_a] += count

    # Output recommendations with counts
    for user in user_recommendations:
        recommendations = user_recommendations[user]
        # Sort recommendations first by count descending, then by user ID ascending
        sorted_recommendations = sorted(
            recommendations.items(),
            key=lambda x: (-x[1], int(x[0]))
        )
        top_recommendations = [f"{rec[0]}:{rec[1]}" for rec in sorted_recommendations[:10]]
        print(f"{user}\t{','.join(top_recommendations)}")

if __name__ == "__main__":
    reduce_recommendations()