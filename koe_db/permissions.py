from django.core.exceptions import ObjectDoesNotExist
from .models import AccessLevel, CustomTable, IndicatorPermission, Indicator, Data, CustomIndicator, ActionLog

def initialize_indicator_access(indicator, access_level=AccessLevel.PUBLIC):
    """
    Initialize an indicator's access level after creation
    """
    AccessLevel.objects.create(indicator=indicator, level=access_level)

def check_indicator_permission(user, indicator, permission_type):
    """
    Check if a user has specified permission for an indicator

    Args:
        user: UserAccount instance
        indicator: Indicator instance
        permission_type: 'view', 'edit', or 'delete'

    Returns:
        bool: Whether the user has permission
    """
    if user.is_superuser:
        return True

    try:
        access = indicator.access_level
    except (AttributeError, ObjectDoesNotExist):
        # Default to public if no access level is set
        initialize_indicator_access(indicator)
        access = indicator.access_level

    # Public indicators: everyone can view
    if permission_type == 'view' and access.level == AccessLevel.PUBLIC:
        return True

    # Unrestricted access: everyone has full permissions
    if access.level == AccessLevel.UNRESTRICTED:
        return True

    # Organization members: access for @ucy.ac.cy emails
    if access.level == AccessLevel.ORGANIZATION and user.email.endswith('@ucy.ac.cy'):
        return True

    if access.level == AccessLevel.ORG_FULL_PUBLIC:
        if permission_type == 'view':
            return True
        if user.email.endswith('@ucy.ac.cy'):
            return True


    # For restricted access, check user-specific permissions
    if access.level == AccessLevel.RESTRICTED:
        try:
            user_perm = IndicatorPermission.objects.get(user=user, indicator=indicator)
            if permission_type == 'view':
                return user_perm.can_view
            elif permission_type == 'edit':
                return user_perm.can_edit
            elif permission_type == 'delete':
                return user_perm.can_delete
        except IndicatorPermission.DoesNotExist:
            return False

    # Default deny
    return False


def check_custom_indicator_permission(user, custom_indicator, permission_type):
    """
    Check if user has permission for a custom indicator

    For view access: needs view access to the custom indicator itself AND all base indicators
    For edit/delete: needs appropriate permission on custom indicator AND all base indicators
    """
    # First check permission on the custom indicator itself
    if not check_indicator_permission(user, custom_indicator.indicator, permission_type):
        return False

    # Then check permissions on all base indicators
    for base in custom_indicator.base_indicators.all():
        # For view, only need view permission on base indicators
        if permission_type in ('edit', 'delete'):
            if not check_indicator_permission(user, base, permission_type):
                return False
        else:  # For view
            if not check_indicator_permission(user, base, 'view'):
                return False

    return True

def check_table_view_permission(user, table):
    """
    Check if a user has permission to view a table

    Args:
        user: UserAccount instance
        table: Table instance

    Returns:
        bool: Whether the user has permission
    """
    if user.is_superuser:
        return True

    # Check if user has permission on all indicators in the table
    for indicator in table.indicators.all():
        if not check_indicator_permission(user, indicator, 'view'):
            return False

    return True

def get_accessible_tables(user):
    """Get all tables a user can view"""
    if user.is_superuser:
        return CustomTable.objects.all()

    # Start with all tables that have public indicators
    public_tables = CustomTable.objects.filter(indicators__access_level__level=AccessLevel.PUBLIC)

    # Add tables with unrestricted access
    unrestricted_tables = CustomTable.objects.filter(indicators__access_level__level=AccessLevel.UNRESTRICTED)

    # Add tables with organization access
    org_tables = CustomTable.objects.none()
    if user.email.endswith('@ucy.ac.cy'):
        org_tables = CustomTable.objects.filter(indicators__access_level__level=AccessLevel.ORGANIZATION)

    org_full_public_tables = CustomTable.objects.filter(indicators__access_level__level=AccessLevel.ORG_FULL_PUBLIC)
    # Add specifically permitted tables
    permitted_ids = IndicatorPermission.objects.filter(
        user=user,
        can_view=True
    ).values_list('indicator__custom_tables', flat=True)

    restricted_tables = CustomTable.objects.filter(id__in=permitted_ids)

    # Use union to efficiently combine querysets
    result = public_tables.union(
        unrestricted_tables,
        org_tables,
        org_full_public_tables,
        restricted_tables
    )

    return result

def get_accessible_indicators(user):
    """Get all indicators a user can view"""
    if user.is_superuser:
        return Indicator.objects.all()

    # Start with all publicly viewable indicators
    public_indicators = Indicator.objects.filter(access_level__level=AccessLevel.PUBLIC)
    unrestricted_indicators = Indicator.objects.filter(access_level__level=AccessLevel.UNRESTRICTED)
    org_full_public_indicators = Indicator.objects.filter(access_level__level=AccessLevel.ORG_FULL_PUBLIC)
    # Add organization indicators if user is from the organization
    org_indicators = Indicator.objects.none()
    if user.email.endswith('@ucy.ac.cy'):
        org_indicators = Indicator.objects.filter(access_level__level=AccessLevel.ORGANIZATION)
    # Add specifically permitted indicators
    permitted_ids = IndicatorPermission.objects.filter(
        user=user,
        can_view=True
    ).values_list('indicator_id', flat=True)

    restricted_indicators = Indicator.objects.filter(id__in=permitted_ids)

    # Use union to efficiently combine querysets
    result = public_indicators.union(
        unrestricted_indicators,
        org_full_public_indicators,
        org_indicators,
        restricted_indicators
    )


    return result
