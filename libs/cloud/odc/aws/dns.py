""" Tools for interacting with route53
"""
from . import _fetch_text, ec2_tags, mk_boto_session


def public_ip():
    return _fetch_text("http://instance-data/latest/meta-data/public-ipv4")


def _find_zone_id(domain, route53):
    zone_name = ".".join(domain.split(".")[1:])
    zone_name = zone_name.rstrip(".") + "."
    rr = route53.list_hosted_zones()
    for z in rr["HostedZones"]:
        if z["Name"] == zone_name:
            return z["Id"]

    return None


def dns_update(domain, ip=None, route53=None, ttl=300):
    if route53 is None:
        route53 = mk_boto_session().create_client("route53")

    domain = domain.rstrip(".") + "."
    zone_id = _find_zone_id(domain, route53)

    if zone_id is None:
        return False

    if ip is None:
        ip = public_ip()

    update = {
        "Name": domain,
        "Type": "A",
        "TTL": ttl,
        "ResourceRecords": [{"Value": ip}],
    }
    changes = {"Changes": [{"Action": "UPSERT", "ResourceRecordSet": update}]}

    rr = route53.change_resource_record_sets(HostedZoneId=zone_id, ChangeBatch=changes)

    return rr["ResponseMetadata"]["HTTPStatusCode"] == 200


def dns_delete(domain, route53=None):
    if route53 is None:
        route53 = mk_boto_session().create_client("route53")

    domain = domain.rstrip(".") + "."
    zone_id = _find_zone_id(domain, route53)

    if zone_id is None:
        return False

    rr = route53.list_resource_record_sets(
        HostedZoneId=zone_id, StartRecordType="A", StartRecordName=domain
    ).get("ResourceRecordSets", [])
    if len(rr) < 1:
        return False
    rec = rr[0]
    if rec.get("Name", "") != domain:
        return False

    changes = {"Changes": [{"Action": "DELETE", "ResourceRecordSet": rec}]}
    rr = route53.change_resource_record_sets(HostedZoneId=zone_id, ChangeBatch=changes)
    return rr["ResponseMetadata"]["HTTPStatusCode"] == 200


def cli(args):
    def error(msg):
        print(msg, file=sys.stderr)

    def display_help():
        print(
            """Modify DNS record of EC2 instance:

arguments: domain_name|tag/<tag name containing domain name> [auto|delete|ip]

Examples:
  test.devbox.dea.ga.gov.au
  test.devbox.dea.ga.gov.au auto
  test.devbox.dea.ga.gov.au 3.44.10.22
  tag/domain auto
  tag/domain delete
"""
        )

    n = len(args)
    if n == 0:
        display_help()
        return 0
    elif n == 1:
        if args[0] in ("help", "--help"):
            display_help()
            return 0
        args = (args[0], "auto")
    elif n > 2:
        error("Too many arguments, expect: domain [ip|auto|delete]")
        return 1

    domain, ip = args

    if domain.startswith("tag/"):
        tag = domain[4:]
        tags = ec2_tags()
        if tags is None:
            error("Unable to query tags")
            return 2
        domain = tags.get(tag)
        if domain is None:
            error('No such tag: "{}"'.format(tag))
            return 3

    if ip == "auto":
        ip = public_ip()
        if ip is None:
            error("Unable to find public IP of this EC2 instance")
            return 4
    if ip == "delete":
        print("Deleting record for domain: {}".format(domain))
        ok = dns_delete(domain)
    else:
        print("Updating record for {} to {}".format(domain, ip))
        ok = dns_update(domain, ip)

    if not ok:
        error("FAILED")
        return 1

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(cli(sys.argv[1:]))
