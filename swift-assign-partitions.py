import argparse
import math

from swift.common import ring


def assign_partitions(in_builder, out_builder, region, parts_ratio):
    """Assigns `parts_ratio` partitions to `region`.
    * `in_builder` is the name of file storing initial builder.
    * `out_builder` is the name of file to write resulting builder to.

    """
    builder = ring.RingBuilder.load(in_builder)

    r1_weight = sum([dev['weight'] for dev in builder.devs if dev['region'] != region])
    r2_weight = (parts_ratio * r1_weight) / (1 - parts_ratio)

    r2_devs = [dev for dev in builder.devs if dev['region'] == region]
    dev_weight = r2_weight / len(r2_devs)
    # rounding weight to 2 decimals
    dev_weight= math.floor(dev_weight*100) / 100
    
    # Update weight for every device in specified region
    for dev in r2_devs:
        print "Setting new weight of {} to device {}".format(dev_weight, dev['id'])
        builder.set_dev_weight(dev['id'], dev_weight)
    
    builder.save(out_builder)


def parse_args():
    desc = "Set weights according to the ratio of partitions to assign "
    "to the new region."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("input_builder",
                        help="Ring builder to start from.")
    parser.add_argument("output_builder",
                        help="Output file to store new builder.")
    parser.add_argument("region", type=int,
                        help="Region in which nodes will be updated.")
    parser.add_argument("ratio", type=float,
                        help="Ratio of partitions to assign to the region "
                             "for instance 0.01 for 1%%")
    return parser.parse_args()


def main():
    args = parse_args()
    assign_partitions(args.input_builder, args.output_builder,
                      args.region, args.ratio)


if __name__ == "__main__":
    main()
