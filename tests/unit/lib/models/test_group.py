from datetime import datetime, timezone
from uuid import UUID, uuid4

from src.lib.groups import Group, GroupType
from src.lib.models.group import GroupDBModel


def test_make() -> None:
    group_type = GroupType.TextGroup
    name = "group name"
    item1 = "some item"
    item2 = "another item"
    date = datetime.fromtimestamp(0)
    uuid = uuid4()
    group = Group(
        group_type=group_type,
        name=name,
        items={item1, item2},
        date=date,
        group_id=uuid
    )
    model = GroupDBModel.make(group)
    assert model.group_type == group_type
    assert model.name == name
    assert model.items in {f"{item1}|{item2}", f"{item2}|{item1}"}
    assert model.date == "0.0"
    assert model.group_id == uuid


def test_make_defaults() -> None:
    before = datetime.now().timestamp()
    group_type = GroupType.TextGroup
    name = "group name"
    item1 = "some item"
    item2 = "another item"
    group = Group(
        group_type=group_type,
        name=name,
        items={item1, item2}
    )
    model = GroupDBModel.make(group)
    after = datetime.now().timestamp()
    assert model.group_type == group_type
    assert model.name == name
    assert model.items in {f"{item1}|{item2}", f"{item2}|{item1}"}
    assert before < float(model.date) < after
    assert isinstance(model.group_id, UUID)


def test_make_one_item() -> None:
    before = datetime.now().timestamp()
    group_type = GroupType.TextGroup
    name = "group name"
    item1 = "some item"
    group = Group(
        group_type=group_type,
        name=name,
        items={item1}
    )
    model = GroupDBModel.make(group)
    after = datetime.now().timestamp()
    assert model.group_type == group_type
    assert model.name == name
    assert model.items == item1
    assert before < float(model.date) < after
    assert isinstance(model.group_id, UUID)


def test_make_empty_items() -> None:
    before = datetime.now().timestamp()
    group_type = GroupType.TextGroup
    name = "group name"
    group = Group(
        group_type=group_type,
        name=name,
        items=set()
    )
    model = GroupDBModel.make(group)
    after = datetime.now().timestamp()
    assert model.group_type == group_type
    assert model.name == name
    assert model.items == ""
    assert before < float(model.date) < after
    assert isinstance(model.group_id, UUID)


def test_to_group() -> None:
    uuid = uuid4()
    name = "some name"
    model = GroupDBModel(
        group_id=uuid,
        group_type=GroupType.ManuscriptGroup,
        name=name,
        date="0.0",
        items="a|b"
    )
    group = model.to_group()
    assert group.group_id == uuid
    assert group.name == name
    assert group.group_type == GroupType.ManuscriptGroup
    assert group.date == datetime.fromtimestamp(0, timezone.utc)
    assert group.items == {"b", "a"}
