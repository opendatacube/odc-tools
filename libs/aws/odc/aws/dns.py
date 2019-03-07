""" Tools for interacting with route53
"""
from . import get_boto_session, _fetch_text


def public_ip():
    return _fetch_text('http://instance-data/latest/meta-data/public-ipv4')


def _find_zone_id(domain, route53):
    zone_name = '.'.join(domain.split('.')[1:])
    zone_name = zone_name.rstrip('.') + '.'
    rr = route53.list_hosted_zones()
    for z in rr['HostedZones']:
        if z['Name'] == zone_name:
            return z['Id']

    return None


def dns_update(domain, ip=None, route53=None, ttl=300):
    if route53 is None:
        route53 = get_boto_session().create_client('route53')

    domain = domain.rstrip('.') + '.'
    zone_id = _find_zone_id(domain, route53)

    if zone_id is None:
        return False

    if ip is None:
        ip = public_ip()

    update = {
        'Name': domain,
        'Type': 'A',
        'TTL': ttl,
        'ResourceRecords': [{'Value': ip}]
    }
    changes = {"Changes": [{"Action": "UPSERT",
                            "ResourceRecordSet": update}]}

    rr = route53.change_resource_record_sets(HostedZoneId=zone_id,
                                             ChangeBatch=changes)

    return rr['ResponseMetadata']['HTTPStatusCode'] == 200


def dns_delete(domain, route53=None):
    if route53 is None:
        route53 = get_boto_session().create_client('route53')

    domain = domain.rstrip('.') + '.'
    zone_id = _find_zone_id(domain, route53)

    if zone_id is None:
        return False

    rr = route53.list_resource_record_sets(HostedZoneId=zone_id,
                                           StartRecordType='A',
                                           StartRecordName=domain).get('ResourceRecordSets', [])
    if len(rr) < 1:
        return False
    rec = rr[0]
    if rec.get('Name', '') != domain:
        return False

    changes = {"Changes": [{"Action": "DELETE",
                            "ResourceRecordSet": rec}]}
    rr = route53.change_resource_record_sets(HostedZoneId=zone_id,
                                             ChangeBatch=changes)
    return rr['ResponseMetadata']['HTTPStatusCode'] == 200
