import dataclasses
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lib.groups import Group, GroupType
from lib.database.sqlite.models import Groups


@pytest.fixture
def group() -> Group:
    group_type = GroupType.TextGroup
    name = "group name"
    date = datetime.fromtimestamp(0)
    uuid = uuid4()
    return Group(
        group_type=group_type,
        name=name,
        items=set(),
        date=date,
        group_id=uuid
    )


@pytest.fixture
def group_default() -> Group:
    group_type = GroupType.TextGroup
    name = "default group name"
    return Group(
        group_type=group_type,
        name=name,
        items=set(),
    )


@pytest.fixture
def group_model() -> Groups:
    name = "some name"
    return Groups(
        group_type=GroupType.ManuscriptGroup,
        name=name,
        date="0.0",
        items=""
    )


def test_groups_make_multiple_items(group: Group) -> None:
    item1 = "some item"
    item2 = "another item"
    group = dataclasses.replace(group, items={item1, item2})
    model = Groups.make(group)
    assert model.group_type == group.group_type
    assert model.name == group.name
    assert model.items in {f"{item1}|{item2}", f"{item2}|{item1}"}
    assert model.date == "0.0"
    assert model.group_id == group.group_id


def test_groups_make_one_item(group: Group) -> None:
    item = "item"
    group = dataclasses.replace(group, items={item})
    model = Groups.make(group)
    assert model.group_type == group.group_type
    assert model.name == group.name
    assert model.items == item
    assert model.date == "0.0"
    assert model.group_id == group.group_id


def test_groups_make_empty_items(group: Group) -> None:
    model = Groups.make(group)
    assert model.group_type == group.group_type
    assert model.name == group.name
    assert model.items == ""
    assert model.date == "0.0"
    assert model.group_id == group.group_id


def test_groups_make_with_defaults(group_default: Group) -> None:
    model = Groups.make(group_default)
    after = datetime.now().timestamp()
    assert model.group_type == group_default.group_type
    assert model.name == group_default.name
    assert model.items == ""
    assert float(model.date) < after
    assert after - float(model.date) < 5  # should never be 5 seconds
    assert model.group_id == group_default.group_id


def test_groups_to_group_empty_items(group_model: Groups) -> None:
    group = group_model.to_group()
    assert group.group_id == group_model.group_id
    assert group.name == group_model.name
    assert group.group_type == GroupType.ManuscriptGroup
    assert group.date == datetime.fromtimestamp(0, timezone.utc)
    assert group.items == set()


def test_groups_to_group_one_item(group_model: Groups) -> None:
    group_model.items = "a"
    group = group_model.to_group()
    assert group.group_id == group_model.group_id
    assert group.name == group_model.name
    assert group.group_type == GroupType.ManuscriptGroup
    assert group.date == datetime.fromtimestamp(0, timezone.utc)
    assert group.items == {"a"}


def test_groups_to_group_multiple_items(group_model: Groups) -> None:
    group_model.items = "a|b"
    group = group_model.to_group()
    assert group.group_id == group_model.group_id
    assert group.name == group_model.name
    assert group.group_type == GroupType.ManuscriptGroup
    assert group.date == datetime.fromtimestamp(0, timezone.utc)
    assert group.items == {"b", "a"}
