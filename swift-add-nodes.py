import argparse
import math

from swift.common import ring

HDDs = ['sdb1', 'sdc1', 'sdd1', 'sde1']

def add_nodes(in_builder, out_builder, region, nodes_ips, port, hdds):
    """Add a node to the builder identified by `in_builder`.
    * `out_builder` is the name of file to write resulting builder to.
    * `nodes_ips` is the list of nodes' IPs
    * `region` is the region where to add the nodes
    * `port` is the port of the Swift service (usually 6000 for
    objects, 6001 for containers and 6002 for accounts)
    * `hdds` is the list of devices on the node (for instance `[sdb1,
    sdc1, ...]`). Each device will have one partition assigned.

    """
    builder = ring.RingBuilder.load(in_builder)
    r1_weight = sum([dev['weight'] for dev in builder.devs if dev['region'] != region])
    cur_r2_weight = sum([dev['weight'] for dev in builder.devs if dev['region'] == region])

    # We've got the following equality r1_parts/r1_weight = r2_parts/r2_weight
    # Let's say we want 1 partition per device on one new node

    cur_r2_devices = len([dev for dev in builder.devs if dev['region'] == region])
    new_r2_devices = len(HDDs) * len(nodes_ips)
    tot_r2_devices = cur_r2_devices + new_r2_devices
    
    r1_parts = builder.parts*builder.replicas - tot_r2_devices
    r2_parts = tot_r2_devices  ## Putting 1 partition / device
    tot_r2_weight = r2_parts*r1_weight / r1_parts
    new_r2_weight = tot_r2_weight - cur_r2_weight
    newdev_weight = new_r2_weight / new_r2_devices
    # rounding weight to 2 decimals en bit lower to avoid overassignment.
    newdev_weight= math.floor(newdev_weight*99.9) / 100

    for ip in nodes_ips:
        for dev in hdds:
            devdict = {'weight': newdev_weight, 'region': 2, 'zone': 0,
                       'ip': ip, 'port': port, 'device': dev}
            print "Adding device: {}".format(devdict)
            builder.add_dev(devdict)

    builder.save(out_builder)


def parse_args():
    desc = "Add Swift nodes to a region."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("input_builder",
                        help="Ring builder to start from.")
    parser.add_argument("output_builder",
                        help="Output file to store new builder.")
    parser.add_argument("region", type=int,
                        help="Region to add nodes to.")
    parser.add_argument("port", type=int,
                        help="Port of Swift service (usually 6000 for objects, "
                             "6001 for containers and 6002 for accounts.")
    parser.add_argument("nodes_ips", metavar="IP", nargs='+',
                        help="IPs of nodes to add to region.")
    return parser.parse_args()


def main():
    args = parse_args()
    add_nodes(args.input_builder, args.output_builder,
              args.region, args.nodes_ips, args.port, HDDs)


if __name__ == "__main__":
    main()
