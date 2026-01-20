from networkx.algorithms import community

def evaluate_partition(G, communities, method_name):
    if not communities or len(communities) == 0:
        print(f"No communities found by {method_name}.")
        return

    partition = [set(c) for c in communities]

    print(f"Evaluating {method_name} ---")
    try:
        mod = community.modularity(G, partition)
        print(f"Modularity: {mod:.4f}")
    except Exception as e:
        print(f"Could not calculate Modularity: {e}")

    try:
        qual = community.partition_quality(G, partition)
        print(f"Partition Quality (Coverage, Performance): ({qual[0]:.4f}, {qual[1]:.4f})")
    except Exception as e:
        print(f"Could not calculate Partition Quality: {e}")
    print(f"Found {len(partition)} communities.")
    print("-" * 30)